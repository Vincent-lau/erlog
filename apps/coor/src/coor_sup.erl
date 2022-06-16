-module(coor_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(ProgName) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [ProgName]).

start_link(ProgName, TmpPath) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [ProgName, TmpPath]).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init(Args) ->
    SupFlags =
        #{strategy => one_for_all,
          intensity => 0,
          period => 1},
    ChildSpecs =
        [#{id => coordinator,
           start => {coordinator, start_link, [self() | Args]},
           shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
