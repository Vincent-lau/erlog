-module(db_ops).

-compile(export_all).

-include("data_repr.hrl").

-spec get_rel_by_name(dl_db_instance(), string()) -> dl_db_instance().
get_rel_by_name(DBInstance, Name) ->
  lists:filter(fun(#dl_atom{pred_sym = N}) -> Name =:= N end, DBInstance).

%% this uses the underlying assumption that an erlang record is a tuple
%%
% -spec relation_arity(tuple()) -> integer().
% relation_arity(R) ->
%   tuple_size(R) - 1.

-spec project(dl_db_instance(), [integer()]) -> dl_db_instance().
project(DBInstance, Cols) ->
  lists:map(fun(Atom = #dl_atom{args = Args}) ->
               Atom#dl_atom{args = utils:filteri(fun(I) -> lists:member(I, Cols) end, Args)}
            end,
            DBInstance).

-spec join(dl_knowledgebase(), string(), string(), integer(), integer(), string()) ->
            [dl_atom()].
join(Kb, Pred_sym1, Pred_sym2, C1, C2, ResName) ->
  Atoms1 = lists:filter(fun(#dl_atom{pred_sym = Sym}) -> Pred_sym1 =:= Sym end, Kb),
  Atoms2 = lists:filter(fun(#dl_atom{pred_sym = Sym}) -> Pred_sym2 =:= Sym end, Kb),
  lists:flatmap(fun(Atom) -> join_one(Atoms2, C1, C2, ResName, Atom) end, Atoms1).

-spec join_one([dl_atom()], integer(), integer(), string(), dl_atom()) -> [dl_atom()].
join_one(Dl_atoms, C1, C2, ResName, #dl_atom{args = Args}) ->
  Nth = lists:nth(C1, Args),
  Selected_atoms =
    lists:filter(fun(#dl_atom{args = Args2}) -> lists:nth(C2, Args2) =:= Nth end, Dl_atoms),
  Selected_args =
    lists:map(fun(#dl_atom{args = Args2}) ->
                 {L, [_ | R]} = lists:split(C2 - 1, Args2),
                 L ++ R
              end,
              Selected_atoms),
  lists:map(fun(Args2) -> #dl_atom{pred_sym = ResName, args = Args ++ Args2} end,
            Selected_args).
