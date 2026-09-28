{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  packages = with pkgs; [
    # Existing TypeScript services and dashboard
    nodejs_22
    pnpm

    # Existing Python notification service
    python312
    uv

    # Go backend migration and development
    go
    gopls
    golangci-lint
    delve
    air
    sqlc
    go-migrate # was: migrate

    # Local infrastructure and Kubernetes workflows
    docker
    docker-compose
    postgresql_16
    redis
    kubectl
    kind

    # Common development utilities and native build prerequisites
    git
    curl
    jq
    openssl
    pkg-config
    gcc
  ];
}
