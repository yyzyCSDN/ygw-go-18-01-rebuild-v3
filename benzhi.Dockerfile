FROM golang:1.23

WORKDIR /app

COPY go.mod ./
ENV GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=off
RUN go mod download

COPY . .
RUN go build ./...

ENV GOPROXY=off \
    GOSUMDB=off

CMD ["bash"]
