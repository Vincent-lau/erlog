%%%-------------------------------------------------------------------
%% @doc erlog public API
%% @end
%%%-------------------------------------------------------------------

-module(erlog_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    % XXX allowing access to all not a good idea
    ets:new(table, [named_table, public]),
    erlog_sup:start_link().

stop(_State) ->
    ets:delete(table),
    ok.

%% internal functions
