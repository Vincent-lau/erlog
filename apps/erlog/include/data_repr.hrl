-type dl_const() :: string().
-type dl_var() :: string().
-type dl_term() :: dl_const() | dl_var().

-record(dl_atom, {pred_sym :: dl_const(), args :: [dl_term()]}).

-type dl_atom() :: #dl_atom{}.

-record(dl_pred, {neg :: boolean(), atom :: dl_atom()}).

-type dl_pred() :: #dl_pred{}.

-record(dl_rule, {head, body}).

-type dl_rule() :: #dl_rule{head :: dl_atom(), body :: [dl_pred()]}.
-type dl_program() :: [dl_rule()].
-type dl_db_instance() :: gb_sets:set(dl_atom()).
