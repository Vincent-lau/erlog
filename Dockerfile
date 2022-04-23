# syntax=docker/dockerfile:1
FROM erlang
WORKDIR /erlog
COPY . .
RUN rebar3 clean
RUN rebar3 compile
CMD ["rebar3", "shell"]
EXPOSE 3000
