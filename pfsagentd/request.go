package main

import (
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

func doHTTPRequest(request *http.Request, okStatusCodes ...int) (response *http.Response, responseBody []byte, ok bool) {
	var (
		err              error
		okStatusCode     int
		okStatusCodesSet map[int]struct{}
		retryIndex       uint64
		swiftAuthToken   string
	)

	okStatusCodesSet = make(map[int]struct{})
	for _, okStatusCode = range okStatusCodes {
		okStatusCodesSet[okStatusCode] = struct{}{}
	}

	retryIndex = 0

	for {
		swiftAuthToken, _ = fetchAuthTokenAndAccountURL()

		request.Header.Add("X-Auth-Token", swiftAuthToken)

		response, err = globals.httpClient.Do(request)
		if nil != err {
			logErrorf("doHTTPRequest() failed to submit request: %v", err)
			ok = false
			return
		}

		if nil != request.Body {
			err = request.Body.Close()
			if nil != err {
				logErrorf("doHTTPRequest() failed to close body: %v", err)
				ok = false
				return
			}
		}

		responseBody, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if nil != err {
			logErrorf("doHTTPRequest() failed to read responseBody: %v", err)
			ok = false
			return
		}

		_, ok = okStatusCodesSet[response.StatusCode]
		if ok {
			return
		}

		if retryIndex >= globals.config.SwiftRetryLimit {
			logWarnf("doHTTPRequest() reached SwiftRetryLimit")
			ok = false
			return
		}

		if http.StatusUnauthorized == response.StatusCode {
			logInfof("doHTTPRequest() needs to call updateAuthTokenAndAccountURL()")
			updateAuthTokenAndAccountURL()
		} else {
			logWarnf("doHTTPRequest() needs to retry due to unexpected http.Status %s (%d)", response.Status, response.StatusCode)
		}

		time.Sleep(globals.retryDelay[retryIndex])
		retryIndex++
	}
}

func fetchAuthTokenAndAccountURL() (swiftAuthToken string, swiftAccountURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			swiftAccountURL = globals.swiftAccountURL
			globals.Unlock()
			return
		}

		globals.Unlock()

		swiftAuthWaitGroup.Wait()
	}
}

func updateAuthTokenAndAccountURL() {
	var (
		err                         error
		getRequest                  *http.Request
		getResponse                 *http.Response
		swiftAuthToken              string
		swiftAccountURL             string
		swiftStorageAccountURLSplit []string
		swiftStorageURL             string
	)

	globals.Lock()

	if nil != globals.swiftAuthWaitGroup {
		globals.Unlock()

		_, _ = fetchAuthTokenAndAccountURL()

		return
	}

	globals.swiftAuthWaitGroup = &sync.WaitGroup{}
	globals.swiftAuthWaitGroup.Add(1)

	globals.Unlock()

	getRequest, err = http.NewRequest("GET", globals.config.SwiftAuthURL, nil)
	if nil != err {
		logFatal(err)
	}

	getRequest.Header.Add("X-Auth-User", globals.config.SwiftAuthUser)
	getRequest.Header.Add("X-Auth-Key", globals.config.SwiftAuthKey)

	getResponse, err = globals.httpClient.Do(getRequest)
	if nil != err {
		logErrorf("updateAuthTokenAndAccountURL() failed to submit request: %v", err)
		swiftAuthToken = ""
		swiftAccountURL = ""
	} else {
		_, err = ioutil.ReadAll(getResponse.Body)
		getResponse.Body.Close()
		if nil != err {
			logErrorf("updateAuthTokenAndAccountURL() failed to read responseBody: %v", err)
			swiftAuthToken = ""
			swiftAccountURL = ""
		} else {
			if http.StatusOK != getResponse.StatusCode {
				logWarnf("updateAuthTokenAndAccountURL() got unexpected http.Status %s (%d)", getResponse.Status, getResponse.StatusCode)
				swiftAuthToken = ""
				swiftAccountURL = ""
			} else {
				swiftAuthToken = getResponse.Header.Get("X-Auth-Token")
				swiftStorageURL = getResponse.Header.Get("X-Storage-Url")

				swiftStorageAccountURLSplit = strings.Split(swiftStorageURL, "/")
				if 0 == len(swiftStorageAccountURLSplit) {
					swiftAccountURL = ""
				} else {
					swiftStorageAccountURLSplit[len(swiftStorageAccountURLSplit)-1] = globals.config.SwiftAccountName
					swiftAccountURL = strings.Join(swiftStorageAccountURLSplit, "/")
				}
			}
		}
	}

	globals.Lock()

	globals.swiftAuthToken = swiftAuthToken
	globals.swiftAccountURL = swiftAccountURL

	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil

	globals.Unlock()
}
