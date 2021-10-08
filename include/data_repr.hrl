
-type dl_const() :: atom().
-type dl_var() :: string().
-type dl_term() :: dl_const() | dl_var().

-record(dl_atom, {pred_sym :: atom(), args :: [dl_term()]}).

-type dl_atom() :: #dl_atom{pred_sym :: string(), args :: [dl_term()]}.

-record(dl_rule, {head, body}).

-type dl_rule() :: #dl_rule{head :: dl_atom(), body :: [dl_atom()]}.
-type dl_program() :: [dl_rule()].
-type dl_knowledgebase() :: [dl_atom()].
-type dl_db_instance() :: [dl_atom()].