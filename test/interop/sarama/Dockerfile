FROM golang:1.23-alpine

WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go mod tidy && go build -o /sarama .

ENTRYPOINT ["/sarama"]
