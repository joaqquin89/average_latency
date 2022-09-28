FROM golang:alpine
RUN mkdir /app
COPY . /app
WORKDIR /app
EXPOSE 8000

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main . 
CMD ["/app/main"]