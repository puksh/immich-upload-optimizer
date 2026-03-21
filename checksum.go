package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
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
var fakeToOriginalChecksum map[string]map[string]struct{}
var checksumFileLock sync.Mutex

func isValidSHA1Base64(s string) bool {
	if len(s) != 28 {
		return false
	}
	b, err := base64.StdEncoding.DecodeString(s)
	return err == nil && len(b) == sha1.Size
}

func initChecksums() {
	checksumFileLock.Lock()
	defer checksumFileLock.Unlock()

	loaded := make(map[string]map[string]struct{})
	validLineSet := make(map[string]struct{})
	hadCorruption := false

	file, err := os.OpenFile(checksumsFile, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		mapLock.Lock()
		fakeToOriginalChecksum = loaded
		mapLock.Unlock()
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Default Scanner token limit is 64 KiB. Corrupted tails can exceed this.
	// Increase limit so we can safely skip malformed oversized lines.
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
		if _, ok := loaded[fake]; !ok {
			loaded[fake] = make(map[string]struct{})
		}
		loaded[fake][original] = struct{}{}
		validLineSet[fake+","+original] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading csv:", err)
		hadCorruption = true
	}

	mapLock.Lock()
	fakeToOriginalChecksum = loaded
	mapLock.Unlock()

	if hadCorruption {
		validLines := make([]string, 0, len(validLineSet))
		for line := range validLineSet {
			validLines = append(validLines, line)
		}
		sort.Strings(validLines)
		if err := rewriteChecksumsFile(validLines); err != nil {
			fmt.Println("Error cleaning corrupted checksums:", err)
		} else {
			fmt.Println("Removed corrupted checksum entries from file")
		}
	}
}

func rewriteChecksumsFile(validLines []string) error {
	dir := filepath.Dir(checksumsFile)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(checksumsFile)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	// renamed: the rename has completed; defer must not remove tmpPath (it no longer exists at that path).
	// destRemoved: checksumsFile has been deleted but the rename hasn't succeeded yet;
	// defer must NOT remove tmpPath so the data survives on disk as a recovery artifact.
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
	if err = tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err = tmpFile.Close(); err != nil {
		return err
	}

	if err = os.Rename(tmpPath, checksumsFile); err != nil {
		// Best-effort Windows-friendly fallback: remove destination then retry.
		if removeErr := os.Remove(checksumsFile); removeErr != nil {
			return err
		}
		// Destination is gone. From here on the defer must not delete tmpPath on
		// failure — it is the only remaining copy of the valid checksum data.
		destRemoved = true
		if err = os.Rename(tmpPath, checksumsFile); err != nil {
			return fmt.Errorf("rename failed after removing destination (%s still contains valid data): %w", tmpPath, err)
		}
	}
	renamed = true

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
		newPair := false
		mapLock.Lock()
		if fakeToOriginalChecksum == nil {
			fakeToOriginalChecksum = make(map[string]map[string]struct{})
		}
		if _, ok := fakeToOriginalChecksum[fake]; !ok {
			fakeToOriginalChecksum[fake] = make(map[string]struct{})
		}
		if _, exists := fakeToOriginalChecksum[fake][original]; !exists {
			fakeToOriginalChecksum[fake][original] = struct{}{}
			newPair = true
		}
		mapLock.Unlock()
		if newPair {
			_ = appendToCSV(fake, original)
		}
	}()
}

// toOriginalChecksum: Must acquire mapLock.RLock() before calling
func toOriginalChecksum(checksum string) (string, bool) {
	originals, ok := fakeToOriginalChecksum[checksum]
	if !ok || len(originals) != 1 {
		return "", false
	}
	for original := range originals {
		return original, true
	}
	return "", false
}

func appendToCSV(key, value string) error {
	checksumFileLock.Lock()
	defer checksumFileLock.Unlock()

	file, err := os.OpenFile(checksumsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
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
			if original, ok := toOriginalChecksum(checksum); ok {
				//fmt.Printf("checksum: %s -> %s\n", checksum, original)
				asset["checksum"] = original
			}
		}
	}
}

func getChecksumReplacer(w http.ResponseWriter, r *http.Request, logger *customLogger) *Replacer {
	if isStreamSync(r) {
		return &Replacer{w, r, logger, TypeStream}
	}
	if isFullSync(r) {
		return &Replacer{w, r, logger, TypeFull}
	}
	if isDeltaSync(r) {
		return &Replacer{w, r, logger, TypeDelta}
	}
	/*
		Since immich server v1.133.1
		- Albums don't come with assets on the web (?withoutAssets=true by default) but still do for the app
		- Buckets don't hold assets anymore
	*/
	if isAlbum(r) {
		return &Replacer{w, r, logger, TypeAlbum}
	}
	/*
		if isBucket(r) {
			return &Replacer{w, r, logger, TypeBucket}
		}
	*/
	if isAssetView(r) {
		return &Replacer{w, r, logger, TypeAssetView}
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
	TypeDelta
	TypeFull
	TypeBucket
	TypeAssetView
	TypeStream
)

func (replacer Replacer) Replace() (err error) {
	w, r, logger := replacer.w, replacer.r, replacer.logger
	var req *http.Request
	var resp *http.Response
	if req, err = http.NewRequest(r.Method, upstreamURL+r.URL.String(), nil); logger.Error(err, "new POST") {
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
		case TypeStream:
			fixedJsonBuf := make([]byte, len(jsonBuf)+1)
			fixedJsonBuf[0] = '['
			copy(fixedJsonBuf[1:], replaceAllBytes(jsonBuf, []byte("\n"), []byte(",")))
			fixedJsonBuf[len(fixedJsonBuf)-1] = ']'
			var streams []any
			if err = json.Unmarshal(fixedJsonBuf, &streams); logger.Error(err, "json unmarshal") {
				return
			}
			var filteredStreams []any
			for _, value := range streams {
				if v, ok := value.(map[string]any); ok {
					if t, ok := v["type"].(string); ok && !slices.Contains([]string{"AssetV1", "AlbumAssetCreateV1", "AlbumAssetUpdateV1", "AlbumAssetBackfillV1", "PartnerAssetV1", "PartnerAssetBackfillV1"}, t) {
						filteredStreams = append(filteredStreams, value)
						continue
					}
					if asset, ok := v["data"].(map[string]any); ok {
						mapLock.RLock()
						Asset(asset).toOriginalAsset()
						mapLock.RUnlock()
					}
				}
				filteredStreams = append(filteredStreams, value)
			}
			if jsonBuf, err = json.Marshal(filteredStreams); logger.Error(err, "json marshal") {
				return
			}
			if len(jsonBuf) > 0 {
				jsonBuf = jsonBuf[1:]
				jsonBuf[len(jsonBuf)-1] = '\n'
			}
			replaceAllBytes(jsonBuf, []byte("},{"), []byte("}\n{"))
		case TypeDelta:
			assetsKey = "upserted"
			fallthrough
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
		case TypeBucket:
			fallthrough
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
