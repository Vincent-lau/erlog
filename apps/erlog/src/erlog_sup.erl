%%%-------------------------------------------------------------------
%% @doc erlog top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erlog_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(ProgName) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [ProgName]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([ProgName]) ->
    SupFlags =
        #{strategy => one_for_all,
          intensity => 0,
          period => 1},
    ChildSpecs =
        [#{id => main,
           start => {erlog_srv, start_link, [ProgName]},
           shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}};
init([]) ->
    SupFlags =
        #{strategy => one_for_all,
          intensity => 0,
          period => 1},
    ChildSpecs =
        [#{id => main,
           start => {erlog_srv, start_link, []},
           shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
