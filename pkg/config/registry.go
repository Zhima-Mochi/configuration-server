package config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	registeredKeyPrefix = "/registered/"

	configPrefix = "/config/"
)

var (
	ErrKeyIsRegistered = errors.New("key is registered")
)

type Registry struct {
	etcdClient *clientv3.Client
}

func NewRegistry(endpoints []string) *Registry {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal("etcd client error: ", err)
	}
	return &Registry{etcdClient: cli}
}

func (r *Registry) Register(ctx context.Context, key, filePath string) error {
	resp, err := r.etcdClient.Get(ctx, fmt.Sprintf("%s%s", registeredKeyPrefix, key))
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		_, err = r.etcdClient.Put(ctx, fmt.Sprintf("%s%s", registeredKeyPrefix, key), filePath)
		if err != nil {
			return err
		}
	} else {
		return ErrKeyIsRegistered
	}

	return nil
}

func (r *Registry) Unregister(ctx context.Context, key string) error {
	_, err := r.etcdClient.Delete(ctx, fmt.Sprintf("%s%s", registeredKeyPrefix, key))
	if err != nil {
		return err
	}
	return nil
}

func (r *Registry) Watch(ctx context.Context, key string) (clientv3.WatchChan, error) {
	watchChan := r.etcdClient.Watch(ctx, fmt.Sprintf("%s%s", configPrefix, key))
	return watchChan, nil
}

func (r *Registry) GetConfig(ctx context.Context, key string) ([]byte, error) {
	resp, err := r.etcdClient.Get(ctx, fmt.Sprintf("%s%s", configPrefix, key))
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil
}

func (r *Registry) GetRegisteredKeys(ctx context.Context) ([]string, error) {
	resp, err := r.etcdClient.Get(ctx, registeredKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		keys = append(keys, key[len(registeredKeyPrefix):])
	}
	return keys, nil
}
