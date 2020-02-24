FROM golang:alpine as builder

RUN apk update && apk add git && apk add ca-certificates

WORKDIR /root
RUN mkdir /root/app
COPY go.mod go.sum *.go /root/
COPY app/*.go /root/app/
RUN go get -d -v
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm go build -a app/mysensors.go

FROM scratch

COPY --from=builder /root/mysensors /root/
EXPOSE 9001
ENTRYPOINT ["/root/mysensors", "--broker=tcp://127.0.0.1:1883"]

