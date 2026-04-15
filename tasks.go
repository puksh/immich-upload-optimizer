package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"time"
)

type TaskProcessor struct {
	Task              *Task
	OriginalFile      *os.File
	OriginalFilename  string
	OriginalExtension string
	OriginalSize      int64

	tempOriginalFilePath string

	ProcessedFile      *os.File
	ProcessedFilename  string
	ProcessedExtension string
	ProcessedSize      int64

	tempWorkDir string

	logger *customLogger
}

func NewTaskProcessor(file *os.File, fileName string, fileSize int64, logger *customLogger) (*TaskProcessor, error) {
	originalExtension := path.Ext(fileName)
	if !isValidFilename(originalExtension) {
		return nil, fmt.Errorf("invalid file extension: %s", originalExtension)
	}

	// Must have a task, passthrough the request to immich otherwise
	checkExt := strings.ToLower(strings.TrimPrefix(originalExtension, "."))
	var task *Task
	for _, t := range config.Tasks {
		if slices.Contains(t.Extensions, checkExt) {
			task = t
			break
		}
	}
	if task == nil {
		return nil, fmt.Errorf("no task found for file extension .%s", checkExt)
	}

	if fileSize < task.MinFilesizeBytes {
		return nil, fmt.Errorf("file size is smaller than minimum: %d < %d", fileSize, task.MinFilesizeBytes)
	}

	return &TaskProcessor{
		Task:                 task,
		OriginalFile:         file,
		OriginalFilename:     fileName,
		OriginalExtension:    originalExtension,
		OriginalSize:         fileSize,
		tempOriginalFilePath: file.Name(),
		logger:               logger,
	}, nil
}

// Backward-compatible constructor used by upload code paths that still operate on multipart.File.
func NewTaskProcessorFromMultipart(file multipart.File, header *multipart.FileHeader) (*TaskProcessor, error) {
	originalExtension := path.Ext(header.Filename)
	if !isValidFilename(originalExtension) {
		return nil, fmt.Errorf("invalid file extension: %s", originalExtension)
	}

	originalFile, err := os.CreateTemp("", "upload-*"+originalExtension)
	if err != nil {
		return nil, fmt.Errorf("unable to create temp file: %w", err)
	}

	if _, err = io.Copy(originalFile, file); err != nil {
		_ = originalFile.Close()
		_ = os.Remove(originalFile.Name())
		return nil, fmt.Errorf("unable to write temp file: %w", err)
	}

	if _, err = originalFile.Seek(0, io.SeekStart); err != nil {
		_ = originalFile.Close()
		_ = os.Remove(originalFile.Name())
		return nil, fmt.Errorf("unable to seek temp file: %w", err)
	}

	return NewTaskProcessor(originalFile, header.Filename, header.Size, nil)
}

func (tp *TaskProcessor) SetLogger(logger *customLogger) {
	tp.logger = logger
}

func (tp *TaskProcessor) logf(str string, args ...interface{}) {
	if tp.logger != nil {
		tp.logger.Printf(str, args...)
	}
}

func (tp *TaskProcessor) Close() error {
	_ = tp.CleanOriginalFile()
	return tp.CleanWorkDir()
}

func (tp *TaskProcessor) CleanOriginalFile() (err error) {
	if tp.OriginalFile != nil {
		err = tp.OriginalFile.Close()
		if err != nil {
			tp.logf("unable to close original file: %v", err)
		}
		tp.OriginalFile = nil
	}

	if tp.tempOriginalFilePath != "" {
		err = os.Remove(tp.tempOriginalFilePath)
		if err != nil {
			tp.logf("unable to remove temp file: %v", err)
		}
		tp.tempOriginalFilePath = ""
	}
	return
}

func (tp *TaskProcessor) CleanWorkDir() error {
	if tp.tempWorkDir == "" {
		return nil
	}
	err := os.RemoveAll(tp.tempWorkDir)
	if err != nil {
		tp.logf("unable to clean temp folder: %v", err)
	}
	tp.tempWorkDir = ""
	return err
}

func (tp *TaskProcessor) Run() error {
	// Limit the number of concurrent tasks running
	if slices.Contains(imageExtensions, strings.ToLower(strings.TrimPrefix(tp.OriginalExtension, "."))) {
		imageSemaphore <- struct{}{}
		defer func() { <-imageSemaphore }()
	} else {
		videoSemaphore <- struct{}{}
		defer func() { <-videoSemaphore }()
	}
	var err error

	tp.tempWorkDir, err = os.MkdirTemp("", "processing-*")
	if err != nil {
		return fmt.Errorf("unable to create temp folder: %w", err)
	}

	basename := path.Base(tp.tempOriginalFilePath)
	extension := path.Ext(basename)
	values := map[string]string{
		"result_folder": tp.tempWorkDir,
		"original_name": base64.StdEncoding.EncodeToString([]byte(tp.OriginalFilename)),
		"folder":        path.Dir(tp.tempOriginalFilePath),
		"name":          strings.TrimSuffix(basename, extension),
		"extension":     strings.TrimPrefix(extension, "."),
	}

	var cmdLine bytes.Buffer
	err = tp.Task.CommandTemplate.Execute(&cmdLine, values)
	if err != nil {
		return fmt.Errorf("unable to generate command to be Run: %w", err)
	}
	tp.logf("running task: %s: %s", tp.Task.Name, cmdLine.String())
	cmd := exec.Command("sh", "-c", cmdLine.String())
	cmd.Dir = path.Dir(configFile)
	setTaskProcessGroup(cmd)

	// Use separate buffers to avoid deadlock on large output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run command with timeout using a goroutine
	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	timeout := time.After(taskTimeout)
	select {
	case err = <-done:
		// Command completed
	case <-timeout:
		killTaskProcessTree(cmd)
		tp.logf("task exceeded timeout of %s, process and all children were killed", taskTimeout)
		return fmt.Errorf("task timeout: process exceeded %s limit while running command:\n%s", taskTimeout, cmdLine.String())
	}

	output := append(stdout.Bytes(), stderr.Bytes()...)
	if err != nil {
		if jxlFallbackToOriginal && isJxlCommand(cmdLine.String()) && isJxlUnsupportedCPUError(err, output) {
			tp.logf("JXL command failed due to unsupported CPU instructions, falling back to original file upload: %v | %s", err, strings.TrimSpace(string(output)))
			if fallbackErr := tp.writeOriginalToResultFolder(); fallbackErr != nil {
				return fmt.Errorf("%w while running command:\n%s\nOutput:\n%s\nFallback error:\n%v", err, cmdLine.String(), string(output), fallbackErr)
			}
		} else {
			return fmt.Errorf("%w while running command:\n%s\nOutput:\n%s", err, cmdLine.String(), string(output))
		}
	}

	files, err := os.ReadDir(tp.tempWorkDir)
	if err != nil {
		return fmt.Errorf("unable to read temp directory: %w", err)
	}

	if len(files) != 1 {
		return fmt.Errorf("unexpected number of files in temp directory: %d", len(files))
	}

	processedFilePath := path.Join(tp.tempWorkDir, files[0].Name())
	tp.ProcessedFile, err = os.Open(processedFilePath)
	if err != nil {
		return fmt.Errorf("unable to open temp file: %w", err)
	}
	stat, err := os.Stat(processedFilePath)
	if err != nil {
		err = fmt.Errorf("unable to get file size: %w", err)
	}
	tp.ProcessedSize = stat.Size()
	tp.ProcessedExtension = path.Ext(processedFilePath)
	tp.ProcessedFilename = strings.TrimSuffix(tp.OriginalFilename, tp.OriginalExtension) + tp.ProcessedExtension

	return nil
}

func isJxlUnsupportedCPUError(err error, output []byte) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error() + "\n" + string(output))
	markers := []string{
		"illegal instruction",
		"signal: illegal instruction",
		"sigill",
		"unsupported cpu",
		"unsupported instruction",
	}
	for _, marker := range markers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func isJxlCommand(cmdLine string) bool {
	cmd := strings.ToLower(cmdLine)
	return strings.Contains(cmd, "cjxl") || strings.Contains(cmd, "djxl")
}

func (tp *TaskProcessor) writeOriginalToResultFolder() error {
	if _, err := tp.OriginalFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to seek original file: %w", err)
	}
	b := path.Base(tp.tempOriginalFilePath)
	dstPath := path.Join(tp.tempWorkDir, b)
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("unable to create fallback output file: %w", err)
	}
	defer dst.Close()
	if _, err = io.Copy(dst, tp.OriginalFile); err != nil {
		return fmt.Errorf("unable to write fallback output file: %w", err)
	}
	return nil
}
