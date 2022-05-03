-module(dbs).

-export([project/2, join/5, get_rel_by_name/2, get_rel_by_name_and_rest/2, rename_pred/2,
         get_rel_by_pred/2]).
-export([new/0, singleton/1, fold/3, map/2, is_empty/1, subtract/2, filteri/2, foreach/2,
         split_args/2, equal/2, union/2, from_list/1, flatten/1, size/1]).
-export([to_string/1]).
-export([read_db/1, read_db/2, write_db/2, write_db/3]).

-import(dl_repr, [cons_const/1, cons_atom/2]).

-include("../include/data_repr.hrl").

%%----------------------------------------------------------------------
%% IO operations of db
%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% @doc
%% Read from a file of atoms with predicate symbols
%% @returns the database instance
%% @end
%%----------------------------------------------------------------------
-spec read_db(string()) -> dl_db_instance().
read_db(FileName) ->
  {ok, Stream} = file:open(FileName, [read]),
  {F, _R} = preproc:lex_and_parse(stream, Stream),
  file:close(Stream),
  dbs:from_list(F).

%%----------------------------------------------------------------------
%% @doc
%% Read from a file of atoms without predicate symbols, and construct the
%% database based on the name QryName
%% @param QryName is the name of <em>all</em> atoms in the db
%% @returns the database instance
%%
%% @end
%%----------------------------------------------------------------------
-spec read_db(string(), string()) -> dl_db_instance().
read_db(FileName, QryName) ->
  {ok, Stream} = file:open(FileName, [read]),
  cons_db_from_data(read_data(Stream), QryName).

read_data(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = erl_scan:string(Line),
      [lists:map(fun({_, _, Args}) -> Args end, Tokens) | read_data(S)]
  end.

cons_db_from_data(Data, AtomName) ->
  dbs:from_list(
    lists:map(fun(Args) -> cons_atom(AtomName, Args) end, Data)).

%%----------------------------------------------------------------------
%% @doc
%% @equiv write_db(FileName, DB, [write])
%% @end
%%----------------------------------------------------------------------
-spec write_db(file:filename(), dl_db_instance()) -> ok.
write_db(FileName, DB) ->
  write_db(FileName, DB, [write]).

-spec write_db(file:filename(), dl_db_instance(), [file:mode()]) -> ok | {error, term()}.
write_db(FileName, DB, Modes) ->
  {ok, Stream} = file:open(FileName, Modes),
  io:format(Stream, "~s~n", [to_string(DB)]),
  file:close(Stream).

%%----------------------------------------------------------------------
%% RA operations
%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% @doc
%% Given a db instance, and columns to project, output the db
%% with all atoms projected. This needs to be order preserving.
%%
%% Example: ordering preserving P(a, b, c) [2,1] -> P(b, a)
%% @end
%%----------------------------------------------------------------------
-spec project(dl_db_instance(), [integer()]) -> dl_db_instance().
project(DBInstance, Cols) ->
  map(fun(Atom) -> project_atom(Atom, Cols) end, DBInstance).

%%----------------------------------------------------------------------
%% @doc
%% Function: project_atom
%% Purpose: given an atom and columns to be projected, project the atom
%% preserving the order
%% @end
%%----------------------------------------------------------------------
-spec project_atom(dl_atom(), [integer()]) -> dl_atom().
project_atom(A = #dl_atom{args = Args}, Cols) ->
  A#dl_atom{args = project_args(Args, Cols)}.

%%----------------------------------------------------------------------
%% @doc
%% Function: project_args
%% Purpose: order preserving argument projection
%% @end
%%----------------------------------------------------------------------
-spec project_args([dl_term()], [integer()]) -> [dl_term()].
project_args(_, []) ->
  [];
project_args(Args, [C | Cs]) ->
  [lists:nth(C, Args) | project_args(Args, Cs)].

%%----------------------------------------------------------------------
%% @doc
%% Given two database instances, and the columns on which we wise to join,
%% join them together to get a new db.
%% @end
%%----------------------------------------------------------------------

-spec join(dl_db_instance(), dl_db_instance(), [integer()], [integer()], dl_const()) ->
            dl_db_instance().
join(DB1, DB2, C1, C2, ResName) ->
  % we create a data structure that maps the cols to join to a database instance
  % with atoms that have those particular cols
  JoinCols =
    fold(fun(Atom1, AccIn) ->
            Args1 = nth_args(lists:sort(C1), dl_repr:get_atom_args(Atom1)),
            ColsDB = maps:get(Args1, AccIn, dbs:new()),
            NewColsDB = add(Atom1, ColsDB),
            maps:put(Args1, NewColsDB, AccIn)
         end,
         #{},
         DB1),
  % then for each atom candidate, we look up from our map to see if there is an atom
  % that can be joined
  % We do this to avoid the O(n^2) pass to our two databases to be joined
  fold(fun(Atom2, AccIn) ->
          Args2 = nth_args(lists:sort(C2), dl_repr:get_atom_args(Atom2)),
          case maps:find(Args2, JoinCols) of
            {ok, ColsDB} ->
              RemoveJoinColArgs =
                listsi:filteri(fun(_, I) -> not lists:member(I, C2) end,
                               dl_repr:get_atom_args(Atom2)),
              JoinedAtoms =
                map(fun(Atom1) ->
                       dl_repr:cons_atom(ResName, dl_repr:get_atom_args(Atom1) ++ RemoveJoinColArgs)
                    end,
                    ColsDB),
              dbs:union(JoinedAtoms, AccIn);
            error -> AccIn
          end
       end,
       dbs:new(),
       DB2).

-spec join_one(dl_db_instance(), dl_atom(), [integer()], [integer()], dl_const()) ->
                dl_db_instance().
join_one(DlAtoms, #dl_atom{args = Args1}, C1, C2, ResName) ->
  SelectedAtoms =
    filter(fun(#dl_atom{args = Args2}) ->
              nth_args(lists:sort(C2), Args2) =:= nth_args(lists:sort(C1), Args1)
           end,
           DlAtoms),
  RemoveJoinColArgs =
    map(fun(A = #dl_atom{args = Args2}) ->
           NewArgs = listsi:filteri(fun(_, I) -> not lists:member(I, C2) end, Args2),
           A#dl_atom{args = NewArgs}
        end,
        SelectedAtoms),
  map(fun(#dl_atom{args = Args2}) -> #dl_atom{pred_sym = ResName, args = Args1 ++ Args2}
      end,
      RemoveJoinColArgs).

% -spec select_before_join(dl_db_instance(),
%                          dl_db_instance(),
%                          [integer()],
%                          [integer()],
%                          dl_const(),
%                          dl_const()) ->
%                           dl_db_instance().
% select_before_join(DB1, DB2, L1, L2, InputName, ResName) ->
%   SelectedAtoms1 = dbs:get_rel_by_name(InputName, DB1),

%%----------------------------------------------------------------------
%% operations on atoms in the db
%%----------------------------------------------------------------------

-spec get_rel_by_name(string(), dl_db_instance()) -> dl_db_instance().
get_rel_by_name(Name, DBInstance) ->
  filter(fun(Atom) -> Name =:= dl_repr:get_atom_name(Atom) end, DBInstance).

-spec get_rel_by_name_and_rest(string(), dl_db_instance()) ->
                                {dl_db_instance(), dl_db_instance()}.
get_rel_by_name_and_rest(Name, DBInstance) ->
  Rel = get_rel_by_name(Name, DBInstance),
  NonRel = filter(fun(Atom) -> Name =/= dl_repr:get_atom_name(Atom) end, DBInstance),
  {Rel, NonRel}.

-spec get_rel_by_pred(dl_pred(), dl_db_instance()) -> dl_db_instance().
get_rel_by_pred(Pred = #dl_pred{}, DBInstance) ->
  case dl_repr:is_neg_pred(Pred) of
    false ->
      get_rel_by_name(dl_repr:get_pred_name(Pred), DBInstance);
    true ->
      neg:neg_pred(
        dl_repr:get_pred_atom(Pred), DBInstance)
  end.

-spec rename_pred(dl_const(), dl_db_instance()) -> dl_db_instance().
rename_pred(NewPred, DB) ->
  map(fun(A) -> A#dl_atom{pred_sym = NewPred} end, DB).

%%----------------------------------------------------------------------
%% operations on db representations
%%----------------------------------------------------------------------

-spec new() -> dl_db_instance().
new() ->
  gb_sets:new().

-spec add(term(), dl_db_instance()) -> dl_db_instance().
add(Ele, DB) ->
  gb_sets:add(Ele, DB).

-spec is_empty(dl_db_instance()) -> boolean().
is_empty(DB) ->
  gb_sets:is_empty(DB).

%%----------------------------------------------------------------------
%% Function: diff
%% Purpose: find all atoms that are in DB1 but not in DB2
%% Args:
%% Returns:
%%
%%----------------------------------------------------------------------
-spec subtract(dl_db_instance(), dl_db_instance()) -> dl_db_instance().
subtract(DB1, DB2) ->
  gb_sets:subtract(DB1, DB2).

-spec filteri(fun((dl_atom(), integer()) -> boolean()), dl_db_instance()) ->
               dl_db_instance().
filteri(Predi, Set) ->
  L = gb_sets:to_list(Set),
  L2 = listsi:filteri(Predi, L),
  gb_sets:from_list(L2).

-spec map(fun((dl_atom()) -> dl_atom()), dl_db_instance()) -> dl_db_instance().
map(Fun, Set) ->
  L = gb_sets:to_list(Set),
  L2 = lists:map(Fun, L),
  gb_sets:from_list(L2).

-spec fold(fun((term(), term()) -> term()), term(), gb_sets:set()) -> term().
fold(Fun, Acc, Set) ->
  gb_sets:fold(Fun, Acc, Set).

%% input Set would be a set of gb_sets
-spec flatmap(fun((term()) -> term()), gb_sets:set()) -> gb_sets:set().
flatmap(Fun, Set) ->
  S2 = map(Fun, Set),
  gb_sets:union(
    gb_sets:to_list(S2)).

filter(Pred, Set) ->
  gb_sets:filter(Pred, Set).

foreach(Fun, Set) ->
  L = gb_sets:to_list(Set),
  lists:foreach(Fun, L).

nth_args([], _) ->
  [];
nth_args([H | T], Args) ->
  [lists:nth(H, Args) | nth_args(T, Args)].

split_args(N, Args) ->
  lists:split(N, Args).

-spec equal(dl_db_instance(), dl_db_instance()) -> boolean().
equal(DB1, DB2) ->
  gb_sets:is_subset(DB1, DB2) andalso gb_sets:is_subset(DB2, DB1).

-spec union(dl_db_instance(), dl_db_instance()) -> dl_db_instance().
union(CurDB, NewDB) ->
  gb_sets:union(CurDB, NewDB).

-spec from_list([dl_atom()]) -> dl_db_instance().
from_list(L) ->
  gb_sets:from_list(L).

-spec flatten([dl_db_instance()]) -> dl_db_instance().
flatten(L) ->
  gb_sets:union(L).

-spec size(dl_db_instance()) -> integer().
size(DB) ->
  gb_sets:size(DB).

-spec singleton(dl_atom()) -> dl_db_instance().
singleton(Key) ->
  gb_sets:singleton(Key).

-spec to_string(dl_db_instance()) -> string().
to_string(DB) ->
  L = gb_sets:to_list(DB),
  lists:join(".\n", lists:map(fun(Atom) -> dl_repr:atom_to_string(Atom) end, L)) ++ ".".
