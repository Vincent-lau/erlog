%%%-------------------------------------------------------------------
%% @doc coor public API
%% @end
%%%-------------------------------------------------------------------

-module(coor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, [ProgName]) ->
    coor_sup:start_link(ProgName).

-spec stop(_) -> 'ok'.
stop(_State) ->
    ok.


%% internal functions