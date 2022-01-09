-module(dconfig).

-compile(export_all).

-record(node_config, {nodes :: #{node() => port()}}).

-type config() :: #node_config{}.

-spec get_node_host(node()) -> string().
get_node_host(NodeName) ->
  element(2, get_node_name_host(NodeName)).

-spec get_node_name(node()) -> string().
get_node_name(NodeName) ->
  element(1, get_node_name_host(NodeName)).

-spec get_node_name_host(node()) -> {string(), string()}.
get_node_name_host(NodeName) ->
  [Name, Host] = string:tokens(atom_to_list(NodeName), "@"),
  {Name, Host}.

-spec is_shortname(node()) -> boolean().
is_shortname(NodeName) ->
  Host = get_node_host(NodeName),
  case string:find(Host, ".") of
    nomatch -> true;
    _S -> false
  end.


%%----------------------------------------------------------------------
%% @doc
%% @param Name is the name of the node
%% <p>
%% This function will check whether the input Name is a short or long name,
%% and return the string cmd to run.
%% </p>
%%
%% <p>
%% For example, if the name is node1@127.0.0.1, which means this is a
%% fully qualified domain name, so this function will return
%% "-name node1@127.0.0.1"
%% </p>
%% @end
%%----------------------------------------------------------------------
-spec get_name_cmd(node()) -> string().
get_name_cmd(NodeName) ->
  case is_shortname(NodeName) of
    true -> "-sname " ++ atom_to_list(NodeName);
    false -> "-name " ++ atom_to_list(NodeName)
  end.

%%----------------------------------------------------------------------
%% @doc
%% @param PA is the path to the executable which can be empty.
%% @end
%%----------------------------------------------------------------------
-spec start_node(atom(), string()) -> port().
start_node(Name, PA) ->
  NameCmd = get_name_cmd(Name),
  Cmd = io_lib:format("erl -noshell -noinput ~s -pa ~s", [NameCmd, PA]),
  erlang:open_port({spawn, Cmd}, []).


-spec stop_node(node(), config()) -> true.
stop_node(NodeName, #node_config{nodes = Nodes}) ->
  erpc:call(NodeName, init, stop, []),
  Port = maps:get(NodeName, Nodes),
  true = erlang:port_close(Port).

-spec get_nodes(config()) -> [node()].
get_nodes(#node_config{nodes = Nodes}) ->
  maps:keys(Nodes).


all_work(Cfg) ->
  multicall(worker, start, [], Cfg),
  multicall(worker, start_working, [], Cfg).

multicall(Module, Function, Args, Cfg) ->
  Nodes = get_nodes(Cfg),
  erpc:multicall(Nodes, Module, Function, Args).

call(Node, Module, Function, Args) ->
  erpc:call(Node, Module, Function, Args).



start_cluster([BaseName], Num) ->
  start_cluster([BaseName], Num, os:cmd("rebar3 path")).

-spec start_cluster(list(), integer(), string()) -> config().
start_cluster([BaseName], Num, PA) ->
  case string:find(atom_to_list(BaseName), "@") of
    nomatch ->
      NodeName = list_to_atom(atom_to_list(BaseName) ++ "@127.0.0.1"),
      start_cluster([NodeName, longnames], Num, PA);
    _S ->
      start_cluster([BaseName, longnames], Num, PA)
    end;
start_cluster([BaseName, longnames], Num, PA) ->
  {Name, Host} = get_node_name_host(BaseName),
  NodeNames = [list_to_atom(Name ++ integer_to_list(N) ++ "@" ++ Host) || N <- lists:seq(1, Num)],
  NodePids = lists:map(fun(N) -> start_node(N, PA) end, NodeNames),
  Nodes = lists:zip(NodeNames, NodePids),
  #node_config{nodes = maps:from_list(Nodes)}.

-spec stop_cluster(config()) -> StopRes when StopRes :: list().
stop_cluster(Cfg = #node_config{nodes = Nodes}) ->
  lists:map(fun (NodeName) -> stop_node(NodeName, Cfg) end, maps:keys(Nodes)).

-spec add_node(atom(), config()) -> config().
add_node(Name, Cfg) ->
  ok.

%%----------------------------------------------------------------------
%% @doc
%% This function will remove the node from the cluster if it is present,
%% and cleanup all the resources that the node has used.
%% @end
%%----------------------------------------------------------------------
-spec remove_node(node(), config()) -> config().
remove_node(Name, Cfg) ->
  ok.


-spec isolate([node()], atom()) -> ok.
isolate(Nodes, Id) ->
  not_impl.

-spec isolate_end([node()]) -> ok.
isolate_end(Nodes) ->
  not_impl.
