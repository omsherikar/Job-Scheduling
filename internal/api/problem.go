package api

import "strconv"

type apiError struct {
	status  int
	title   string
	detail  string
	headers map[string]string
}

func (e *apiError) Error() string { return e.title }

func badRequest(detail string) *apiError {
	return &apiError{status: 400, title: "bad request", detail: detail}
}

func unauthorized() *apiError {
	return &apiError{status: 401, title: "unauthorized"}
}

func forbidden() *apiError {
	return &apiError{status: 403, title: "forbidden", detail: "role too low"}
}

func notFound(what string) *apiError {
	return &apiError{status: 404, title: "not found", detail: what + " not found"}
}

func conflict(detail string) *apiError {
	return &apiError{status: 409, title: "conflict", detail: detail}
}

func tooMany(detail string, retryAfter int) *apiError {
	return &apiError{
		status:  429,
		title:   "queue full",
		detail:  detail,
		headers: map[string]string{"Retry-After": strconv.Itoa(retryAfter)},
	}
}
