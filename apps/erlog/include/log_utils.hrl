-include_lib("kernel/include/logger.hrl").

-define(LOG_TOPIC(Topic, Level, Report),
        ?LOG(Level, Report, #{topic => Topic})).

-define(LOG_TOPIC_DEBUG(Topic, Report), ?LOG_TOPIC(Topic, debug, Report)).






-define(LOG_TOPIC(Topic, Level, Format, Data),
        ?LOG(Level, "Topic:~p " ++ Format, [Topic | Data])).
-define(LOG_TOPIC_DEBUG(Topic, Format, Data), ?LOG_TOPIC(Topic, debug, Format, Data)).
