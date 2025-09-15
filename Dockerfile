FROM golang:1.25.1
WORKDIR /app
COPY . .
RUN go build -o app main.go

RUN useradd -m client
USER client

CMD ["./app"]