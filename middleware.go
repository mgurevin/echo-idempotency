package middleware

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Rediser interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
}

type KeyExtractor func(echo.Context) (string, bool, error)

// IdempotencyConfig defines the config for Idempotency middleware.
type IdempotencyConfig struct {
	// Skipper defines a function to skip middleware.
	Skipper middleware.Skipper

	Rediser Rediser

	// Methods defines a list of HTTP methods that should be works as idempotent.
	// Optional. Default value []string{"POST"}.
	Methods []string `yaml:"methods"`

	// KeyLookup is a string in the form of "<source>:<name>" that is used
	// to extract key from the request.
	// Optional. Default value "header:X-Idempotency-Key".
	// Possible values:
	// - "header:<name>"
	// - "query:<name>"
	// - "form:<name>"
	KeyLookup string `yaml:"key_lookup"`

	KeyLookupFunc KeyExtractor
}

var DefaultIdempotencyConfig = IdempotencyConfig{
	Skipper:   middleware.DefaultSkipper,
	Methods:   []string{http.MethodPost},
	KeyLookup: "header:X-Idempotency-Key",
}

func Idempotency() echo.MiddlewareFunc {
	return IdempotencyWithConfig(DefaultIdempotencyConfig)
}

type reqRecord struct {
	Done            bool                `json:"done"`
	ResponseCode    int                 `json:"response_code"`
	ResponseHeaders map[string][]string `json:"response_headers"`
	ResponseBody    []byte              `json:"response_body"`
}

type bodyDumpResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func IdempotencyWithConfig(config IdempotencyConfig) echo.MiddlewareFunc {
	// Defaults
	if config.Skipper == nil {
		config.Skipper = DefaultIdempotencyConfig.Skipper
	}

	if len(config.Methods) == 0 {
		config.Methods = DefaultIdempotencyConfig.Methods
	}

	if config.KeyLookup == "" {
		config.KeyLookup = DefaultIdempotencyConfig.KeyLookup
	}

	if config.KeyLookupFunc == nil {
		switch parts := strings.Split(config.KeyLookup, ":"); parts[0] {
		case "header":
			config.KeyLookupFunc = keyFromHeader(parts[1])

		case "query":
			config.KeyLookupFunc = keyFromQuery(parts[1])

		case "form":
			config.KeyLookupFunc = keyFromForm(parts[1])

		default:
			panic(fmt.Errorf("invalid idempotency configuration: unknown key lookup `%s`", parts[0]))
		}
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			skip := true
			for _, m := range config.Methods {
				if c.Request().Method == m {
					skip = false
				}
			}

			if skip {
				return next(c)
			}

			idempotencyKey, found, err := config.KeyLookupFunc(c)
			if err != nil {
				return err
			}

			if !found {
				return next(c)
			}

			reqRec := reqRecord{}
			reqKey := fmt.Sprintf("req::%s", idempotencyKey)
			reqData, err := json.Marshal(reqRec)
			if err != nil {
				return err
			}

			setOK, err := config.Rediser.SetNX(c.Request().Context(), reqKey, reqData, 24*time.Hour).Result()
			if err != nil {
				return err
			}

			if setOK {
				resBody := new(bytes.Buffer)
				mw := io.MultiWriter(c.Response().Writer, resBody)
				writer := &bodyDumpResponseWriter{Writer: mw, ResponseWriter: c.Response().Writer}
				c.Response().Writer = writer

				handlerErr := next(c)

				reqRec.Done = true
				reqRec.ResponseCode = c.Response().Status
				reqRec.ResponseHeaders = c.Response().Header()
				reqRec.ResponseBody = resBody.Bytes()

				reqData, err := json.Marshal(reqRec)
				if err != nil {
					return err
				}

				_, err = config.Rediser.Set(c.Request().Context(), reqKey, reqData, redis.KeepTTL).Result()
				if err != nil {
					return err
				}

				return handlerErr
			}

			for {
				reqDataStr, err := config.Rediser.Get(c.Request().Context(), reqKey).Result()
				if err != nil {
					return err
				}

				if err := json.Unmarshal([]byte(reqDataStr), &reqRec); err != nil {
					return err
				}

				if reqRec.Done {
					for k, vArr := range reqRec.ResponseHeaders {
						for _, v := range vArr {
							c.Response().Header().Set(k, v)
						}
					}

					c.Response().WriteHeader(reqRec.ResponseCode)

					_, err = c.Response().Write(reqRec.ResponseBody)
					if err != nil {
						return err
					}

					return nil
				}

				select {
				case <-c.Request().Context().Done():
					return c.Request().Context().Err()

				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
		}
	}
}

// keyFromHeader returns a `KeyExtractor` that extracts key from the request header.
func keyFromHeader(header string) KeyExtractor {
	return func(c echo.Context) (string, bool, error) {
		key := c.Request().Header.Get(header)
		if key == "" {
			return "", false, nil
		}

		return key, true, nil
	}
}

// keyFromQuery returns a `KeyExtractor` that extracts key from the query string.
func keyFromQuery(param string) KeyExtractor {
	return func(c echo.Context) (string, bool, error) {
		key := c.QueryParam(param)
		if key == "" {
			return "", false, nil
		}

		return key, true, nil
	}
}

// keyFromForm returns a `KeyExtractor` that extracts key from the form.
func keyFromForm(param string) KeyExtractor {
	return func(c echo.Context) (string, bool, error) {
		key := c.FormValue(param)
		if key == "" {
			return "", false, nil
		}

		return key, true, nil
	}
}

func (w *bodyDumpResponseWriter) WriteHeader(code int) {
	w.ResponseWriter.WriteHeader(code)
}

func (w *bodyDumpResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *bodyDumpResponseWriter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

func (w *bodyDumpResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
