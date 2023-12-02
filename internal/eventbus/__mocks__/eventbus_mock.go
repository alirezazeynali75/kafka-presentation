package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"gitlab.ecoex.ir/phinix/blockchain/rebalancer/internal/eventbus"
)

type EventPublisherMock struct {
	mock.Mock
}

func (publisher *EventPublisherMock) PublishMessages(
	ctx context.Context,
	message []eventbus.Message,
) error {
	args := publisher.Called(ctx, message)
	return args.Error(0)
}
