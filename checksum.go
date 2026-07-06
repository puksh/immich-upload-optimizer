package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SHA1(file io.ReadSeeker) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("unable to seek beginning of file: %w", err)
	}
	hasher := sha1.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("could not copy file content to hasher: %v", err)
	}
	return base64.StdEncoding.EncodeToString(hasher.Sum(nil)), nil
}

var mapLock sync.RWMutex
var fakeToOriginalChecksum map[string]string
var originalToFakeChecksum map[string]string
var checksumFileLock sync.Mutex

var syncStreamAssetTypes = map[string]struct{}{
	"AssetV2":               {},
	"PartnerAssetV2":        {},
	"PartnerAssetBackfillV2": {},
	"AlbumAssetCreateV2":    {},
	"AlbumAssetUpdateV2":    {},
	"AlbumAssetBackfillV2":   {},
}

const defaultChecksumFilePerm os.FileMode = 0644

func isValidSHA1Base64(s string) bool {
	if len(s) != 28 {
		return false
	}
	b, err := base64.StdEncoding.DecodeString(s)
	return err == nil && len(b) == sha1.Size
}

// setChecksumPair ensures fake<->original 1:1 relation in memory maps.
func setChecksumPair(fake, original string) bool {
	if existingOriginal, ok := fakeToOriginalChecksum[fake]; ok && existingOriginal == original {
		fmt.Println(magenta("Duplicate checksum pair: %s <-> %s", fake, original))
		return false
	}
	if oldOriginal, ok := fakeToOriginalChecksum[fake]; ok && oldOriginal != original {
		fmt.Println(red("Duplicate fake checksum: %s -> %s , %s", fake, oldOriginal, original))
		delete(originalToFakeChecksum, oldOriginal)
	}
	if oldFake, ok := originalToFakeChecksum[original]; ok && oldFake != fake {
		fmt.Println(red("Duplicate orig checksum: %s -> %s , %s", original, oldFake, fake))
		delete(fakeToOriginalChecksum, oldFake)
	}
	fakeToOriginalChecksum[fake] = original
	originalToFakeChecksum[original] = fake
	return true
}

func removeChecksumPair(fake, original string) bool {
	if existingOriginal, ok := fakeToOriginalChecksum[fake]; !ok || existingOriginal != original {
		return false
	}
	if existingFake, ok := originalToFakeChecksum[original]; !ok || existingFake != fake {
		return false
	}
	delete(fakeToOriginalChecksum, fake)
	delete(originalToFakeChecksum, original)
	return true
}

type stagedChecksumPair struct {
	fake           string
	original       string
	alreadyPresent bool
	persisted      bool
}

func stageChecksumPair(fake, original string) *stagedChecksumPair {
	if !isValidSHA1Base64(fake) || !isValidSHA1Base64(original) {
		return nil
	}
	mapLock.Lock()
	defer mapLock.Unlock()
	if existingOriginal, ok := fakeToOriginalChecksum[fake]; ok && existingOriginal == original {
		if existingFake, ok := originalToFakeChecksum[original]; ok && existingFake == fake {
			return &stagedChecksumPair{fake: fake, original: original, alreadyPresent: true, persisted: true}
		}
	}
	setChecksumPair(fake, original)
	return &stagedChecksumPair{fake: fake, original: original}
}

func (pair *stagedChecksumPair) Persist() error {
	if pair == nil || pair.persisted {
		return nil
	}
	if err := appendToCSV(pair.fake, pair.original); err != nil {
		return err
	}
	pair.persisted = true
	return nil
}

func (pair *stagedChecksumPair) Rollback() {
	if pair == nil || pair.persisted || pair.alreadyPresent {
		return
	}
	mapLock.Lock()
	removeChecksumPair(pair.fake, pair.original)
	mapLock.Unlock()
}

func initChecksums() {
	checksumFileLock.Lock()
	defer checksumFileLock.Unlock()

	fakeToOriginalChecksum = make(map[string]string)
	originalToFakeChecksum = make(map[string]string)

	validLineSet := make(map[string]struct{})
	hadCorruption := false
	hadDuplicateLines := false
	hadAmbiguousMappings := false

	file, err := os.OpenFile(checksumsFile, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		kv := strings.SplitN(line, ",", 2)
		if len(kv) != 2 {
			hadCorruption = true
			continue
		}
		fake := strings.TrimSpace(kv[0])
		original := strings.TrimSpace(kv[1])
		if !isValidSHA1Base64(fake) || !isValidSHA1Base64(original) {
			hadCorruption = true
			continue
		}
		canonicalLine := fake + "," + original
		if _, exists := validLineSet[canonicalLine]; exists {
			hadDuplicateLines = true
			continue
		}
		if oldOriginal, ok := fakeToOriginalChecksum[fake]; ok && oldOriginal != original {
			hadAmbiguousMappings = true
		}
		if oldFake, ok := originalToFakeChecksum[original]; ok && oldFake != fake {
			hadAmbiguousMappings = true
		}
		changed := setChecksumPair(fake, original)
		if !changed {
			// exact duplicate pair - already handled.
			continue
		}
		validLineSet[canonicalLine] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(red("Error reading csv: %v", err))
		hadCorruption = true
	}

	if hadCorruption || hadDuplicateLines || hadAmbiguousMappings {
		// Rebuild from canonical in-memory map so stale/overridden historical
		// lines are not written back to disk.
		validLines := make([]string, 0, len(fakeToOriginalChecksum))
		for fake, original := range fakeToOriginalChecksum {
			validLines = append(validLines, fake+","+original)
		}
		sort.Strings(validLines)
		if err := rewriteChecksumsFile(validLines); err != nil {
			fmt.Println(red("Error cleaning corrupted checksums: %v", err))
		}
	}
}

func rewriteChecksumsFile(validLines []string) error {
	dir := filepath.Dir(checksumsFile)
	checksumFilePerm := defaultChecksumFilePerm
	if stat, statErr := os.Stat(checksumsFile); statErr == nil {
		checksumFilePerm = stat.Mode().Perm()
	}
	tmpFile, err := os.CreateTemp(dir, filepath.Base(checksumsFile)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	renamed := false
	destRemoved := false
	defer func() {
		if !renamed && !destRemoved {
			_ = os.Remove(tmpPath)
		}
	}()

	content := ""
	if len(validLines) > 0 {
		content = strings.Join(validLines, "\n") + "\n"
	}
	if _, err = io.WriteString(tmpFile, content); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err = tmpFile.Chmod(checksumFilePerm); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err = tmpFile.Close(); err != nil {
		return err
	}

	if err = os.Rename(tmpPath, checksumsFile); err != nil {
		if removeErr := os.Remove(checksumsFile); removeErr != nil {
			return err
		}
		destRemoved = true
		if err = os.Rename(tmpPath, checksumsFile); err != nil {
			return fmt.Errorf("rename failed after removing destination (%s still contains valid data): %w", tmpPath, err)
		}
	}
	renamed = true
	if chmodErr := os.Chmod(checksumsFile, checksumFilePerm); chmodErr != nil {
		return fmt.Errorf("unable to restore checksum file permissions: %w", chmodErr)
	}

	if dirFd, dirErr := os.Open(dir); dirErr == nil {
		_ = dirFd.Sync()
		_ = dirFd.Close()
	}

	return nil
}

func addChecksums(fake, original string) {
	if !isValidSHA1Base64(fake) || !isValidSHA1Base64(original) {
		return
	}
	go func() {
		mapLock.Lock()
		changed := setChecksumPair(fake, original)
		mapLock.Unlock()
		if changed {
			_ = appendToCSV(fake, original)
		}
	}()
}

func appendToCSV(key, value string) error {
	checksumFileLock.Lock()
	defer checksumFileLock.Unlock()

	file, err := os.OpenFile(checksumsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if chmodErr := file.Chmod(defaultChecksumFilePerm); chmodErr != nil {
		return chmodErr
	}
	if _, err := io.WriteString(file, key+","+value+"\n"); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

type Asset map[string]any

// toOriginalAsset: Must acquire mapLock.RLock() before calling
func (asset Asset) toOriginalAsset() {
	if downloadJpgFromJxl || downloadJpgFromAvif {
		if n, ok := asset["originalFileName"]; ok {
			if originalFileName, ok := n.(string); ok {
				extension := strings.ToLower(path.Ext(originalFileName))
				if (downloadJpgFromJxl && extension == ".jxl") || (downloadJpgFromAvif && extension == ".avif") {
					asset["originalFileName"] = originalFileName + ".jpg"
				}
			}
		}
	}
	if c, ok := asset["checksum"]; ok {
		if checksum, ok := c.(string); ok {
			if original, ok := fakeToOriginalChecksum[checksum]; ok {
				asset["checksum"] = original
			}
		}
	}
}

func rewriteSyncStreamBody(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}

	lines := bytes.Split(body, []byte("\n"))
	rewritten := make([]byte, 0, len(body))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		newLine, err := rewriteSyncStreamLine(line)
		if err != nil {
			return nil, err
		}
		rewritten = append(rewritten, newLine...)
		rewritten = append(rewritten, '\n')
	}

	return rewritten, nil
}

func rewriteSyncStreamLine(line []byte) ([]byte, error) {
	var item map[string]any
	if err := json.Unmarshal(line, &item); err != nil {
		return nil, err
	}

	if t, ok := item["type"].(string); ok {
		if _, shouldRewrite := syncStreamAssetTypes[t]; shouldRewrite {
			if data, ok := item["data"].(map[string]any); ok {
				mapLock.RLock()
				Asset(data).toOriginalAsset()
				mapLock.RUnlock()
			}
		}
	}

	return json.Marshal(item)
}

func doSyncStreamRequest(r *http.Request, bodyBytes []byte) (uploadHTTPResult, error) {
	req, err := http.NewRequest(r.Method, upstreamURL+r.URL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return uploadHTTPResult{}, err
	}
	req.Header = r.Header.Clone()

	resp, err := getHTTPclient().Do(req)
	if err != nil {
		return uploadHTTPResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return uploadHTTPResult{}, err
	}

	return uploadHTTPResult{
		statusCode: resp.StatusCode,
		headers:    resp.Header.Clone(),
		body:       body,
	}, nil
}

func replaceSyncStream(w http.ResponseWriter, r *http.Request, logger *customLogger) error {
	logger.SetErrPrefix("sync-stream")
	var err error
	var bodyBytes []byte
	if bodyBytes, err = io.ReadAll(r.Body); logger.Error(err, "read body") {
		return err
	}

	result, err := doSyncStreamRequest(r, bodyBytes)
	if logger.Error(err, "upstream request") {
		return err
	}
	if result.statusCode != http.StatusOK {
		return writeSyncStreamBody(w, result.headers, result.statusCode, result.body, nil)
	}

	if bodyBytes, err = rewriteSyncStreamBody(result.body); logger.Error(err, "rewrite body") {
		return err
	}

	return writeSyncStreamBody(w, result.headers, result.statusCode, bodyBytes, logger)
}

func writeSyncStreamBody(w http.ResponseWriter, headers http.Header, statusCode int, body []byte, logger *customLogger) error {
	setHeaders(w.Header(), headers)
	if !slices.Contains([]string{"gzip", "br"}, headers.Get("Content-Encoding")) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(statusCode)
	if len(body) == 0 {
		return nil
	}
	if _, err := io.Copy(w, bytes.NewReader(body)); err != nil {
		if logger != nil {
			logger.Error(err, "resp write")
		}
		return err
	}
	return nil
}

type bulkUploadCheckItem struct {
	ID       string `json:"id"`
	Checksum string `json:"checksum"`
}

type bulkUploadCheckRequest struct {
	Assets []bulkUploadCheckItem `json:"assets"`
}

type bulkUploadCheckResult struct {
	ID      string `json:"id"`
	Action  string `json:"action"`
	AssetID string `json:"assetId"`
}

type bulkUploadCheckResponse struct {
	Results []bulkUploadCheckResult `json:"results"`
}

type assetMediaResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func replaceBulkUploadCheck(w http.ResponseWriter, r *http.Request, logger *customLogger) error {
	logger.SetErrPrefix("bulk-upload-check")
	var err error
	var bodyBytes []byte
	if bodyBytes, err = io.ReadAll(r.Body); logger.Error(err, "read body") {
		return err
	}
	var checkReq bulkUploadCheckRequest
	if err = json.Unmarshal(bodyBytes, &checkReq); logger.Error(err, "json unmarshal") {
		return err
	}
	originalAssets, mappedAssets := buildBulkUploadCheckRequests(checkReq)
	bodyBytes, err = json.Marshal(bulkUploadCheckRequest{Assets: originalAssets})
	if logger.Error(err, "json marshal") {
		return err
	}
	originalResult, err := doBulkUploadCheckRequest(r, bodyBytes)
	if logger.Error(err, "upstream request") {
		return err
	}
	if originalResult.statusCode != http.StatusOK {
		return writeRawBulkUploadCheckResponse(w, originalResult)
	}

	var originalResp bulkUploadCheckResponse
	if err = json.Unmarshal(originalResult.body, &originalResp); logger.Error(err, "json unmarshal") {
		return err
	}
	if len(mappedAssets) == 0 {
		return writeRawBulkUploadCheckResponse(w, originalResult)
	}

	mappedBodyBytes, err := json.Marshal(bulkUploadCheckRequest{Assets: mappedAssets})
	if logger.Error(err, "json marshal") {
		return err
	}
	mappedResult, err := doBulkUploadCheckRequest(r, mappedBodyBytes)
	if logger.Error(err, "mapped upstream request") {
		return err
	}
	if mappedResult.statusCode != http.StatusOK {
		return fmt.Errorf("mapped bulk upload check returned status %d", mappedResult.statusCode)
	}

	var mappedResp bulkUploadCheckResponse
	if err = json.Unmarshal(mappedResult.body, &mappedResp); logger.Error(err, "json unmarshal") {
		return err
	}

	originalResults := make(map[string]bulkUploadCheckResult, len(originalResp.Results))
	for _, result := range originalResp.Results {
		originalResults[result.ID] = result
	}
	mappedResults := make(map[string]bulkUploadCheckResult, len(mappedResp.Results))
	for _, result := range mappedResp.Results {
		mappedResults[result.ID] = result
	}

	merged := bulkUploadCheckResponse{Results: make([]bulkUploadCheckResult, 0, len(checkReq.Assets))}
	for _, asset := range checkReq.Assets {
		result, ok := originalResults[asset.ID]
		if !ok {
			continue
		}
		if result.Action == "accept" {
			if mappedResult, ok := mappedResults[asset.ID]; ok && mappedResult.Action == "reject" {
				result = mappedResult
			}
		}
		merged.Results = append(merged.Results, result)
	}

	if bodyBytes, err = json.Marshal(merged); logger.Error(err, "json marshal") {
		return err
	}
	return writeBulkUploadCheckBody(w, originalResult.headers, originalResult.statusCode, bodyBytes, logger)
}

func buildBulkUploadCheckRequests(checkReq bulkUploadCheckRequest) ([]bulkUploadCheckItem, []bulkUploadCheckItem) {
	originalAssets := make([]bulkUploadCheckItem, 0, len(checkReq.Assets))
	mappedAssets := make([]bulkUploadCheckItem, 0, len(checkReq.Assets))

	mapLock.RLock()
	defer mapLock.RUnlock()

	for _, asset := range checkReq.Assets {
		key := normalizeChecksum(asset.Checksum)
		originalAssets = append(originalAssets, bulkUploadCheckItem{ID: asset.ID, Checksum: key})
		if fake, ok := originalToFakeChecksum[key]; ok && fake != key {
			mappedAssets = append(mappedAssets, bulkUploadCheckItem{ID: asset.ID, Checksum: fake})
		}
	}

	return originalAssets, mappedAssets
}

func normalizeChecksum(checksum string) string {
	if raw, err := hex.DecodeString(checksum); err == nil && len(raw) == sha1.Size {
		return base64.StdEncoding.EncodeToString(raw)
	}
	return checksum
}

func doBulkUploadCheckRequest(r *http.Request, bodyBytes []byte) (uploadHTTPResult, error) {
	req, err := http.NewRequest(r.Method, upstreamURL+r.URL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return uploadHTTPResult{}, err
	}
	req.Header = r.Header.Clone()

	resp, err := getHTTPclient().Do(req)
	if err != nil {
		return uploadHTTPResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return uploadHTTPResult{}, err
	}

	return uploadHTTPResult{
		statusCode: resp.StatusCode,
		headers:    resp.Header.Clone(),
		body:       body,
	}, nil
}

func writeRawBulkUploadCheckResponse(w http.ResponseWriter, result uploadHTTPResult) error {
	return writeBulkUploadCheckBody(w, result.headers, result.statusCode, result.body, nil)
}

func writeBulkUploadCheckBody(w http.ResponseWriter, headers http.Header, statusCode int, body []byte, logger *customLogger) error {
	setHeaders(w.Header(), headers)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(statusCode)
	if len(body) == 0 {
		return nil
	}
	if _, err := io.Copy(w, bytes.NewReader(body)); err != nil {
		if logger != nil {
			logger.Error(err, "resp write")
		}
		return err
	}
	return nil
}

func getChecksumReplacer(w http.ResponseWriter, r *http.Request, logger *customLogger) *Replacer {
	if isAlbum(r) {
		return &Replacer{w, r, logger, TypeAlbum}
	}
	if isAssetView(r) {
		return &Replacer{w, r, logger, TypeAssetView}
	}
	if isSearchMetadata(r) {
        return &Replacer{w, r, logger, TypeSearch}
    }
	return nil
}

type Replacer struct {
	w      http.ResponseWriter
	r      *http.Request
	logger *customLogger
	typeId int
}

const (
	TypeAlbum = iota
	TypeFull
	TypeAssetView
	TypeSearch
)

func (replacer Replacer) Replace() (err error) {
	w, r, logger := replacer.w, replacer.r, replacer.logger
	var req *http.Request
	var resp *http.Response
	if req, err = http.NewRequest(r.Method, upstreamURL+r.URL.String(), nil); logger.Error(err, "new request") {
		return
	}
	req.Header = r.Header
	req.Body = r.Body
	if resp, err = getHTTPclient().Do(req); logger.Error(err, "getHTTPclient.Do") {
		return
	}
	defer resp.Body.Close()
	bodyReader, bodyWriter := getBodyWriterReaderHTTP(&w, resp)
	defer bodyReader.Close()
	defer bodyWriter.Close()
	var jsonBuf []byte
	if jsonBuf, err = io.ReadAll(bodyReader); logger.Error(err, "resp read") {
		return
	}
	if resp.StatusCode == http.StatusOK {
		assetsKey := "assets"
		switch replacer.typeId {
		case TypeAlbum:
			var assetsMap map[string]any
			if err = json.Unmarshal(jsonBuf, &assetsMap); logger.Error(err, "json unmarshal") {
				return
			}
			for key, value := range assetsMap {
				if key != assetsKey {
					continue
				}
				if assets, ok := value.([]any); ok {
					var filteredAssets []any
					mapLock.RLock()
					for _, a := range assets {
						if asset, ok := a.(map[string]any); ok {
							Asset(asset).toOriginalAsset()
						}
						filteredAssets = append(filteredAssets, a)
					}
					mapLock.RUnlock()
					assetsMap[key] = filteredAssets
				}
				break
			}
			if jsonBuf, err = json.Marshal(assetsMap); logger.Error(err, "json marshal") {
				return
			}
		case TypeFull:
			var assets []Asset
			if err = json.Unmarshal(jsonBuf, &assets); logger.Error(err, "json unmarshal") {
				return
			}
			mapLock.RLock()
			for _, asset := range assets {
				asset.toOriginalAsset()
			}
			mapLock.RUnlock()
			if jsonBuf, err = json.Marshal(assets); logger.Error(err, "json marshal") {
				return
			}
		case TypeAssetView:
			var asset Asset
			if err = json.Unmarshal(jsonBuf, &asset); logger.Error(err, "json unmarshal") {
				return
			}
			mapLock.RLock()
			asset.toOriginalAsset()
			mapLock.RUnlock()
			if jsonBuf, err = json.Marshal(asset); logger.Error(err, "json marshal") {
				return
			}
		case TypeSearch:
			var resp map[string]any
			if err = json.Unmarshal(jsonBuf, &resp); logger.Error(err, "json unmarshal") {
				return
			}
			if assetsObj, ok := resp[assetsKey].(map[string]any); ok {
				if items, ok := assetsObj["items"].([]any); ok {
					mapLock.RLock()
					for _, it := range items {
						if a, ok := it.(map[string]any); ok {
							Asset(a).toOriginalAsset()
						}
					}
					mapLock.RUnlock()
				}
			}
			if jsonBuf, err = json.Marshal(resp); logger.Error(err, "json marshal") {
				return
			}
		default:
			err = errors.New("invalid replacer type")
			return
		}
	}
	setHeaders(w.Header(), resp.Header)
	if !slices.Contains([]string{"gzip", "br"}, resp.Header.Get("Content-Encoding")) {
		w.Header().Set("Content-Length", strconv.Itoa(len(jsonBuf)))
	}
	w.WriteHeader(resp.StatusCode)
	if _, err = bodyWriter.Write(jsonBuf); logger.Error(err, "resp write") {
		return
	}
	return
}