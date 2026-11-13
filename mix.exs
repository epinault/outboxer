defmodule Outboxer.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/epinault/outboxer"

  def project do
    [
      app: :outboxer,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),

      # Hex
      description:
        "Transactional outbox for Ecto (Postgres and SQLite) with ordered groups and pluggable relays " <>
          "for Kafka, SQS and Oban.",
      package: package(),

      # Docs
      name: "Outboxer",
      source_url: @source_url,
      docs: docs()
    ]
  end

  # No `mod:` on purpose: Outboxer has no application supervisor. You add
  # `{Outboxer, opts}` to your own supervision tree.
  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [precommit: :test, "test.integration": :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:ecto_sql, "~> 3.12"},
      {:nimble_options, "~> 1.1"},
      {:telemetry, "~> 1.0"},

      # Storage adapters: bring the one your Repo uses
      {:postgrex, "~> 0.19", optional: true},
      {:ecto_sqlite3, "~> 0.17", optional: true},

      # Relays: each relay module is only compiled when its dependency is present
      {:brod, "~> 4.0", optional: true},
      {:ex_aws_sqs, "~> 3.4", optional: true},
      {:ex_aws, "~> 2.5", optional: true},
      {:oban, "~> 2.18", optional: true},

      # Needed only by the SQS integration tests (ex_aws HTTP client and XML parser)
      {:req, "~> 0.5", only: [:dev, :test]},
      {:sweet_xml, "~> 0.7", only: :test},

      # Code quality and documentation
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:usage_rules, "~> 1.2", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      "test.integration": ["test --include integration"],
      precommit: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test"
      ]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib guides mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "guides/overview.md",
        "guides/ordering-and-groups.md",
        "guides/storage.md",
        "guides/relay-kafka.md",
        "guides/relay-sqs.md",
        "guides/relay-oban.md",
        "guides/custom-relays.md",
        "guides/operations.md",
        "guides/testing.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [Guides: ~r"guides/"],
      groups_for_modules: [
        Core: [
          Outboxer,
          Outboxer.Message,
          Outboxer.Envelope,
          Outboxer.Migration,
          Outboxer.Testing
        ],
        Relays: [
          Outboxer.Relay,
          Outboxer.Relay.Function,
          Outboxer.Relay.Kafka,
          Outboxer.Relay.SQS,
          Outboxer.Relay.Oban
        ],
        Storage: [Outboxer.Storage, Outboxer.Storage.Postgres, Outboxer.Storage.SQLite]
      ]
    ]
  end
end
