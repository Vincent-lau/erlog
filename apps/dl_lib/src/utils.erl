-module(utils).

-compile(export_all).

-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").


dbg_format(Format, Data) ->
  io:format(standard_error, Format, Data).

dbg_log(Format, Data) ->
  % logger:set_primary_config(level, all),
  ?LOG_DEBUG(Format, Data).
