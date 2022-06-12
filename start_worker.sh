#!/bin/zsh

rebar3 compile
erl -name "worker$1@127.0.0.1" -pa `rebar3 path` -config config/sys.config \
  -eval 'application:ensure_all_started(worker)'
