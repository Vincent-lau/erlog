-type dl_const() :: atom().
-type dl_var() :: string().
-type dl_term() :: dl_const() | dl_var().

-record(dl_atom, {pred_sym, args}).

% TODO change dl_const to string
-type dl_atom() :: #dl_atom{pred_sym :: dl_const(), args :: [dl_term()]}.

-record(dl_rule, {head, body}).

-type dl_rule() :: #dl_rule{head :: dl_atom(), body :: [dl_atom()]}.
-type dl_program() :: [dl_rule()].
-type dl_db_instance() :: gb_sets:set(dl_atom()).
