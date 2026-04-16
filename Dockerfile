FROM golang:1.24-alpine AS builder
WORKDIR /app
# Copy replace-directive modules first (go.mod uses replace ../proto and ../common)
COPY proto/ /proto/
COPY common/ /common/
# Copy provisioner source
COPY provisioner/go.mod provisioner/go.sum ./
RUN go mod download
COPY provisioner/ .
RUN CGO_ENABLED=0 go build -o /provisioner .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /provisioner /provisioner
ENTRYPOINT ["/provisioner"]
