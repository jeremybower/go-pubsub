package pubsub

const Fatal = true
const NonFatal = false

type SubscriptionError struct {
	Err   error
	Fatal bool
}

func NewSubscriptionError(err error, fatal bool) SubscriptionError {
	return SubscriptionError{Err: err, Fatal: fatal}
}

func (e SubscriptionError) Error() string {
	msg := "pubsub subscription: " + e.Err.Error()
	if e.Fatal {
		msg += " (fatal)"
	}

	return msg
}

func (e SubscriptionError) Unwrap() error {
	return e.Err
}

type SubscriptionErrors []SubscriptionError

func (es SubscriptionErrors) Errors() []string {
	var errs []string
	for _, e := range es {
		errs = append(errs, e.Error())
	}
	return errs
}
