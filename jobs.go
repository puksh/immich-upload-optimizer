package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"sync/atomic"
)

var jobIdCounter atomic.Int64
var jobs sync.Map // map[string]*inFlightJob

type uploadHTTPResult struct {
	statusCode int
	headers    http.Header
	body       []byte
	err        error
}

type inFlightJob struct {
	id     int64
	done   chan struct{}
	result uploadHTTPResult
}

func internalErrorResult(message string, err error) uploadHTTPResult {
	if err == nil {
		err = errors.New(message)
	}
	return uploadHTTPResult{
		statusCode: http.StatusInternalServerError,
		headers:    http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		body:       []byte(message + "\n"),
		err:        err,
	}
}

func writeHTTPResult(w http.ResponseWriter, result uploadHTTPResult) error {
	if result.statusCode == 0 {
		result.statusCode = http.StatusInternalServerError
	}
	if result.headers != nil {
		setHeaders(w.Header(), result.headers)
	}
	w.WriteHeader(result.statusCode)
	if len(result.body) == 0 {
		return nil
	}
	_, err := w.Write(result.body)
	if err != nil {
		return fmt.Errorf("unable to write response body: %w", err)
	}
	return nil
}

func buildUploadDedupeKey(file multipart.File, header *multipart.FileHeader) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("unable to seek beginning of upload for dedupe key: %w", err)
	}
	hasher := sha1.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("unable to hash upload for dedupe key: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("unable to reset upload after dedupe key hashing: %w", err)
	}
	hash := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	return fmt.Sprintf("%s,%d", hash, header.Size), nil
}

func newJob(r *http.Request, w http.ResponseWriter, logger *customLogger) (err error) {
	jobID := jobIdCounter.Add(1)
	jobLogger := newCustomLogger(logger, fmt.Sprintf("job %d: ", jobID))
	responseWritten := false
	defer func() {
		if err == nil || responseWritten {
			return
		}
		message := "failed to process file, view IUO logs for more info"
		statusCode := http.StatusInternalServerError
		switch {
		case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
			message = "incomplete upload body received"
			statusCode = http.StatusBadRequest
		case errors.Is(err, http.ErrMissingFile):
			message = "uploaded form did not include file field"
			statusCode = http.StatusBadRequest
		}
		result := internalErrorResult(message, err)
		result.statusCode = statusCode
		if writeErr := writeHTTPResult(w, result); writeErr != nil {
			jobLogger.Printf("failed to write fallback error response: %v", writeErr)
			return
		}
		responseWritten = true
	}()

	formFile, formFileHeader, err := r.FormFile(filterFormKey)
	if err != nil {
		return fmt.Errorf("unable to read file in key %s from uploaded form data: %w", filterFormKey, err)
	}
	defer r.MultipartForm.RemoveAll()
	defer formFile.Close()

	jobDisplayName := fmt.Sprintf("\"%s\" (%s)", formFileHeader.Filename, humanReadableSize(formFileHeader.Size))
	jobLogger.Printf("download original: %s", jobDisplayName)

	jobKey, err := buildUploadDedupeKey(formFile, formFileHeader)
	if err != nil {
		return fmt.Errorf("unable to build dedupe key: %w", err)
	}

	currentJob := &inFlightJob{id: jobID, done: make(chan struct{})}
	actualJob, loaded := jobs.LoadOrStore(jobKey, currentJob)
	if loaded {
		existingJob := actualJob.(*inFlightJob)
		// This upload is a duplicate of an in-flight job. Free temp multipart
		// files immediately before waiting, otherwise retries can fill TMPDIR.
		_ = formFile.Close()
		_ = r.MultipartForm.RemoveAll()
		jobLogger.Printf("duplicate upload detected for %s, waiting for in-flight job %d", jobDisplayName, existingJob.id)
		<-existingJob.done
		if err = writeHTTPResult(w, existingJob.result); err != nil {
			return fmt.Errorf("failed to replay response from in-flight job %d: %w", existingJob.id, err)
		}
		if existingJob.result.err != nil {
			return fmt.Errorf("in-flight job %d failed: %w", existingJob.id, existingJob.result.err)
		}
		return nil
	}
	defer jobs.Delete(jobKey)

	finalized := false
	defer func() {
		if !finalized {
			currentJob.result = internalErrorResult("failed to process file, view IUO logs for more info", errors.New("job did not finalize response"))
			close(currentJob.done)
		}
	}()

	var originalHash string
	var newHash string
	uploadFile := formFile
	uploadFilename := formFileHeader.Filename
	uploadOriginal := true
	var stagedPair *stagedChecksumPair

	taskProcessor, err := NewTaskProcessorFromMultipart(formFile, formFileHeader)
	if err == nil && taskProcessor != nil {
		defer taskProcessor.Close()
		taskProcessor.SetLogger(jobLogger)
		// Delete multipart file before running command. Saves RAM (tmpfs)
		_ = formFile.Close()
		_ = r.MultipartForm.RemoveAll()
		if err = taskProcessor.Run(); err != nil {
			result := internalErrorResult("failed to process file, view IUO logs for more info", fmt.Errorf("failed to process file in job %d: %w", jobID, err))
			currentJob.result = result
			close(currentJob.done)
			finalized = true
			if writeErr := writeHTTPResult(w, result); writeErr != nil {
				return fmt.Errorf("failed to write processing error response: %w", writeErr)
			}
			responseWritten = true
			return result.err
		}
		if taskProcessor.OriginalSize <= taskProcessor.ProcessedSize {
			uploadFile = taskProcessor.OriginalFile
			_ = taskProcessor.CleanWorkDir() // Save RAM before upload (tmpfs)
		} else {
			uploadFile = taskProcessor.ProcessedFile
			uploadFilename = taskProcessor.ProcessedFilename
			uploadOriginal = false
			if originalHash, err = SHA1(taskProcessor.OriginalFile); err != nil {
				return fmt.Errorf("sha1: %w", err)
			}
			_ = taskProcessor.CleanOriginalFile() // Save RAM before upload (tmpfs)
		}
	}
	if !uploadOriginal {
		if newHash, err = SHA1(taskProcessor.ProcessedFile); err != nil {
			return fmt.Errorf("new sha1: %w", err)
		}
		stagedPair = stageChecksumPair(newHash, originalHash)
	}
	// Upload the original file or processed one if a task was found
	result := uploadUpstream(r, uploadFile, uploadFilename)
	currentJob.result = result
	close(currentJob.done)
	finalized = true

	if err = writeHTTPResult(w, result); err != nil {
		stagedPair.Rollback()
		return fmt.Errorf("failed to write upstream response: %w", err)
	}
	responseWritten = true

	if result.err != nil {
		stagedPair.Rollback()
		jobLogger.Printf("upload upstream error: %s", result.err.Error())
		return result.err
	}
	if uploadOriginal {
		jobLogger.Printf("uploaded original: \"%s\" (%s)", formFileHeader.Filename, humanReadableSize(formFileHeader.Size))
	} else {
		if err = stagedPair.Persist(); err != nil {
			return fmt.Errorf("persist checksums: %w", err)
		}
		jobLogger.Printf("uploaded: \"%s\" (%s) <- (%s) \"%s\"", taskProcessor.ProcessedFilename, humanReadableSize(taskProcessor.ProcessedSize), humanReadableSize(taskProcessor.OriginalSize), taskProcessor.OriginalFilename)
	}

	return nil
}

func uploadUpstream(r *http.Request, file io.ReadSeeker, name string) uploadHTTPResult {
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	errChan := make(chan error, 1)
	// Prepare chunked request, this saves A LOT of RAM compared to building the whole buffer in RAM.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer pipeWriter.Close()
		defer multipartWriter.Close()
		for key, values := range r.MultipartForm.Value {
			for _, value := range values {
				if key == "filename" {
					value = name
				}
				if writeErr := multipartWriter.WriteField(key, value); writeErr != nil {
					pipeWriter.CloseWithError(fmt.Errorf("unable to create form data: %w", writeErr))
					errChan <- fmt.Errorf("unable to create form data: %w", writeErr)
					return
				}
			}
		}
		part, err := multipartWriter.CreateFormFile(filterFormKey, name)
		if err != nil {
			pipeWriter.CloseWithError(fmt.Errorf("unable to create form data: %w", err))
			errChan <- fmt.Errorf("unable to create form data: %w", err)
			return
		}
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			pipeWriter.CloseWithError(fmt.Errorf("unable to seek beginning of file: %w", seekErr))
			errChan <- fmt.Errorf("unable to seek beginning of file: %w", seekErr)
			return
		}
		if _, copyErr := io.Copy(part, file); copyErr != nil {
			pipeWriter.CloseWithError(fmt.Errorf("unable to write file in form field: %w", copyErr))
			errChan <- fmt.Errorf("unable to write file in form field: %w", copyErr)
			return
		}
		if closeErr := multipartWriter.Close(); closeErr != nil {
			pipeWriter.CloseWithError(fmt.Errorf("unable to finish form data: %w", closeErr))
			errChan <- fmt.Errorf("unable to finish form data: %w", closeErr)
			return
		}
		errChan <- nil
	}()
	req, err := http.NewRequestWithContext(ctx, "POST", upstreamURL+r.URL.String(), pipeReader)
	if err != nil {
		cancel()
		return internalErrorResult("failed to process file, view IUO logs for more info", fmt.Errorf("unable to create POST request: %w", err))
	}
	req.Header = r.Header
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	// Send the request to the upstream server
	resp, err := getHTTPclient().Do(req)
	if err != nil {
		cancel()
		select {
		case chErr := <-errChan:
			if chErr != nil {
				return internalErrorResult("failed to process file, view IUO logs for more info", fmt.Errorf("error writing data to pipe: %v: %v", err, chErr))
			}
		default:
		}
		return internalErrorResult("failed to process file, view IUO logs for more info", fmt.Errorf("unable to POST: %w", err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return internalErrorResult("failed to process file, view IUO logs for more info", fmt.Errorf("unable to read upstream response body: %w", err))
	}
	return uploadHTTPResult{
		statusCode: resp.StatusCode,
		headers:    resp.Header.Clone(),
		body:       body,
		err:        nil,
	}
}