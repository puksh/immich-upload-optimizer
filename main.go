package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/viper"
)

// goreleaser auto updated vars
var version = "dev"
var commit = "none"
var date = "unknown"

var remote *url.URL
var proxyUrl *url.URL

var maxImageJobs int
var maxVideoJobs int
var taskTimeoutMinutes int
var taskTimeout time.Duration
var imageSemaphore chan struct{}
var videoSemaphore chan struct{}

var showVersion bool
var upstreamURL string
var listenAddr string
var configFile string
var checksumsFile string
var downloadJpgFromJxl bool
var downloadJpgFromAvif bool
var jxlFallbackToOriginal bool
var devMITMproxy bool
var forceColors bool

var config *Config

func init() {
	viper.SetEnvPrefix("iuo")
	viper.AutomaticEnv()
	viper.BindEnv("upstream")
	viper.BindEnv("listen")
	viper.BindEnv("tasks_file")
	viper.BindEnv("download_jpg_from_jxl")
	viper.BindEnv("download_jpg_from_avif")
	viper.BindEnv("jxl_fallback_to_original")
	viper.BindEnv("max_image_jobs")
	viper.BindEnv("max_video_jobs")
	viper.BindEnv("task_timeout_minutes")
	viper.BindEnv("dev_mitm_proxy")
	viper.BindEnv("force_colors")

	viper.SetDefault("upstream", "")
	viper.SetDefault("listen", ":2284")
	viper.SetDefault("tasks_file", "config/lossy_avif.yaml")
	viper.SetDefault("checksums_file", "checksums.csv")
	viper.SetDefault("download_jpg_from_jxl", false)
	viper.SetDefault("download_jpg_from_avif", false)
	viper.SetDefault("jxl_fallback_to_original", true)
	viper.SetDefault("max_image_jobs", 5)
	viper.SetDefault("max_video_jobs", 1)
	viper.SetDefault("task_timeout_minutes", 120)
	viper.SetDefault("dev_mitm_proxy", false)
	viper.SetDefault("force_colors", true)

	flag.BoolVar(&showVersion, "version", false, "Show the current version")
	flag.StringVar(&upstreamURL, "upstream", viper.GetString("upstream"), "Upstream URL. Example: http://immich-server:2283")
	flag.StringVar(&listenAddr, "listen", viper.GetString("listen"), "Listening address")
	flag.StringVar(&configFile, "tasks_file", viper.GetString("tasks_file"), "Path to the configuration file")
	flag.StringVar(&checksumsFile, "checksums_file", viper.GetString("checksums_file"), "Path to the checksums file")
	flag.BoolVar(&downloadJpgFromJxl, "download_jpg_from_jxl", viper.GetBool("download_jpg_from_jxl"), "Converts JXL images to JPG on download for wider compatibility")
	flag.BoolVar(&downloadJpgFromAvif, "download_jpg_from_avif", viper.GetBool("download_jpg_from_avif"), "Converts AVIF images to JPG on download for wider compatibility")
	flag.BoolVar(&jxlFallbackToOriginal, "jxl_fallback_to_original", viper.GetBool("jxl_fallback_to_original"), "If JXL conversion fails due to unsupported CPU instructions, upload the original file instead of failing")
	flag.IntVar(&maxImageJobs, "max_image_jobs", viper.GetInt("max_image_jobs"), "Max number of image jobs running concurrently")
	flag.IntVar(&maxVideoJobs, "max_video_jobs", viper.GetInt("max_video_jobs"), "Max number of video jobs running concurrently")
	flag.IntVar(&taskTimeoutMinutes, "task_timeout_minutes", viper.GetInt("task_timeout_minutes"), "Timeout in minutes for each processing task before killing it")
	flag.BoolVar(&devMITMproxy, "dev_mitm_proxy", viper.GetBool("dev_mitm_proxy"), "Route upstream HTTP traffic through local MITM proxy at http://localhost:8080 (development only)")
	flag.BoolVar(&forceColors, "force_colors", viper.GetBool("force_colors"), "Force colored output even in non-TTY environments like Docker")
	flag.Parse()

	if forceColors {
		color.NoColor = false
	}

	if showVersion {
		fmt.Println(printVersion())
		os.Exit(0)
	}

	validateInput()
	if taskTimeoutMinutes <= 0 {
		log.Fatal("task_timeout_minutes must be greater than 0")
	}
	taskTimeout = time.Duration(taskTimeoutMinutes) * time.Minute
	DevMITMproxy = devMITMproxy

	proxyUrl, _ = url.Parse("http://localhost:8080")
	imageSemaphore = make(chan struct{}, maxImageJobs)
	videoSemaphore = make(chan struct{}, maxVideoJobs)
	initChecksums()
}

var baseLogger *log.Logger
var proxy *httputil.ReverseProxy

// DevMITMproxy can be enabled explicitly with -dev_mitm_proxy / IUO_DEV_MITM_PROXY.
var DevMITMproxy bool

func main() {
	baseLogger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	log.Print(green("Starting %s on %s...", printVersion(), listenAddr))
	tmpDir := os.Getenv("TMPDIR")
	if tmpDir != "" {
		info, err := os.Stat(tmpDir)
		if err == nil && info.IsDir() {
			log.Print(cyan("tmp directory: %s", tmpDir))
			_ = removeAllContents(tmpDir)
		} else {
			panic("TMPDIR must be a directory")
		}
	} else {
		log.Print(yellow("no tmp directory set, uploaded files will be saved on disk, this will shorten your disk lifespan!"))
	}
	// Proxy
	proxy = httputil.NewSingleHostReverseProxy(remote)
	if DevMITMproxy {
		proxy.Transport = http.DefaultTransport
		proxy.Transport.(*http.Transport).Proxy = http.ProxyURL(proxyUrl)
	}
	server := &http.Server{Addr: listenAddr, Handler: http.HandlerFunc(handleRequest)}
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(red("Error starting immich-upload-optimizer: %v", err))
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	var err error
	logger := newCustomLogger(baseLogger, fmt.Sprintf("%s: ", strings.Split(r.RemoteAddr, ":")[0]))
	if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		upgradeWebSocketRequest(w, r, logger)
		return
	}
	defer func() {
		// Only print URL if the request was handled by IUO
		if logger.HasErrPrefix() {
			logger.Printf("request URL: %s", r.URL.String())
		}
	}()
	if downloadJpgFromJxl || downloadJpgFromAvif {
		if ok, assetUUID := isOriginalDownloadPath(r); ok {
			if err = downloadAndConvertImage(w, r, logger, assetUUID[1]); err == nil {
				return
			}
		}
	}
	switch {
	case err != nil:
		break
	case isAssetsUpload(r):
		err = newJob(r, w, logger)
		logger.SetErrPrefix("job err")
		logger.Error(err, "")
		return
	case isBulkUploadCheck(r):
		if err = replaceBulkUploadCheck(w, r, logger); err == nil {
			return
		}
	default:
		if replacer := getChecksumReplacer(w, r, logger); replacer != nil {
			logger.SetErrPrefix(fmt.Sprintf("replacer %d", replacer.typeId))
			err = replacer.Replace()
			logger.Error(err, "")
			return
		}
	}
	r.Host = remote.Host
	proxy.ServeHTTP(w, r)
}

const (
	NONE = iota
	JXL
	AVIF
)

func downloadAndConvertImage(w http.ResponseWriter, r *http.Request, logger *customLogger, assetUUID string) (err error) {
	logger.SetErrPrefix("download and convert")
	var req *http.Request
	var resp *http.Response
	if req, err = http.NewRequest(r.Method, upstreamURL+"/api/assets/"+assetUUID, nil); logger.Error(err, "new GET") {
		return
	}
	req.Header = r.Header
	if resp, err = getHTTPclient().Do(req); logger.Error(err, "getHTTPclient.Do") {
		return
	}
	defer resp.Body.Close()
	bodyReader, _ := getBodyWriterReaderHTTP(nil, resp)
	defer bodyReader.Close()
	var jsonBuf []byte
	if jsonBuf, err = io.ReadAll(bodyReader); logger.Error(err, "resp read") {
		return
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("not HTTP ok")
	}
	var asset Asset
	if err = json.Unmarshal(jsonBuf, &asset); logger.Error(err, "json unmarshal") {
		return
	}
	mimeType := NONE
	if n, ok := asset["originalMimeType"]; ok {
		if originalMimeType, ok := n.(string); ok {
			if downloadJpgFromJxl && originalMimeType == "image/jxl" {
				mimeType = JXL
			} else if downloadJpgFromAvif && originalMimeType == "image/avif" {
				mimeType = AVIF
			}
		}
	}
	if mimeType == NONE {
		return errors.New("no conversion needed")
	}
	// Download file and convert
	logger.Printf("converting to jpg: %s", r.URL)
	var blob *os.File
	if req, err = http.NewRequest("GET", upstreamURL+r.URL.String(), nil); logger.Error(err, "new GET") {
		return
	}
	setHeaders(req.Header, r.Header)
	req.Header.Del("Range")
	if resp, err = getHTTPclient().Do(req); logger.Error(err, "getHTTPclient.Do") {
		return
	}
	if blob, err = os.CreateTemp("", "blob-*"); logger.Error(err, "blob create") {
		return
	}
	cleanupBlob := func() { blob.Close(); _ = os.Remove(blob.Name()) }
	defer cleanupBlob()
	if _, err = io.Copy(blob, resp.Body); logger.Error(err, "blob copy") {
		return
	}
	resp.Body.Close()
	if _, err = blob.Seek(0, io.SeekStart); logger.Error(err, "blob seek") {
		return
	}
	signature := make([]byte, 12)
	if _, err = blob.Read(signature); logger.Error(err, "blob read") {
		return
	}
	var output []byte
	var open *os.File
	switch mimeType {
	case JXL:
		if !bytes.Equal(signature, []byte{0x00, 0x00, 0x00, 0x0C, 0x4A, 0x58, 0x4C, 0x20, 0x0D, 0x0A, 0x87, 0x0A}) {
			return errors.New("bad jxl signature")
		}
		cmd := exec.Command("djxl", blob.Name(), blob.Name()+".jpg")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err = cmd.Run(); logger.Error(err, "djxl") {
			return
		}
		output = append(stdout.Bytes(), stderr.Bytes()...)
	case AVIF:
		if !bytes.Equal(signature[4:], []byte("ftypavif")) {
			return errors.New("bad avif signature")
		}
		cmd := exec.Command("avifdec", "-q", "95", blob.Name(), blob.Name()+".jpg")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err = cmd.Run(); logger.Error(err, "avifdec") {
			return
		}
		output = append(stdout.Bytes(), stderr.Bytes()...)
	default:
		return errors.New("should never happen")
	}
	logger.Print(green("conversion complete: %s", strings.ReplaceAll(string(output), "\n", " - ")))
	cleanupBlob()
	if open, err = os.Open(blob.Name() + ".jpg"); logger.Error(err, "open jpg") {
		return
	}
	defer func() { open.Close(); _ = os.Remove(open.Name()) }()
	// Forward upstream headers without Accept-Ranges and with .jpg filename extension
	setHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Encoding")
	w.Header().Del("Transfer-Encoding")
	w.Header().Del("Accept-Ranges")
	w.Header().Set("Content-Type", "image/jpeg")
	if cd := w.Header().Get("Content-Disposition"); cd != "" {
		if mediaType, params, parseErr := mime.ParseMediaType(cd); parseErr == nil {
			if filename, ok := params["filename"]; ok && filename != "" {
				ext := path.Ext(filename)
				if ext == "" {
					params["filename"] = filename + ".jpg"
				} else {
					params["filename"] = strings.TrimSuffix(filename, ext) + ".jpg"
				}
			}
			w.Header().Set("Content-Disposition", mime.FormatMediaType(mediaType, params))
		}
	}
	if fi, statErr := open.Stat(); statErr == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	}
	if _, err = io.Copy(w, open); logger.Error(err, "write resp") {
		return
	}
	return nil
}
