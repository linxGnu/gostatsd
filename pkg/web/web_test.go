package web

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestHttpServerShutsdown(t *testing.T) {
	testCtx, completed := testContext(t)
	defer completed()

	hs, err := newHttpServer(
		logrus.StandardLogger(),
		nil,
		"TestHttpServerShutsdown",
		"127.0.0.1:0", // should pick a random port to bind to
		false,
		false,
		false,
		true,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(testCtx)
	chDone := make(chan struct{}, 1)
	go func() {
		hs.Run(ctx)
		chDone <- struct{}{}
	}()

	cancel()
	select {
	case <-testCtx.Done():
	case <-chDone:
	}
}
