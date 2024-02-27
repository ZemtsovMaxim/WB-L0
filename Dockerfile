FROM golang:latest

ENV GO111MODULE=on

WORKDIR /app

COPY . .

WORKDIR /app/cmd

RUN go build -o main .

CMD sleep 10 && /app/cmd/main
