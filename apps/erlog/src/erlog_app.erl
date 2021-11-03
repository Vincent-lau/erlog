%%%-------------------------------------------------------------------
%% @doc erlog public API
%% @end
%%%-------------------------------------------------------------------

-module(erlog_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    erlog_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
