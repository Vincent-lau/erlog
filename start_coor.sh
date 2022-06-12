#!/bin/zsh


rebar3 compile

erl -name 'coor@127.0.0.1' -pa `rebar3 path` -config config/sys.config \
  -eval 'application:ensure_all_started(coor)'


