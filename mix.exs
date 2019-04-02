defmodule Otto.MixProject do
  use Mix.Project

  def project do
    [
      app: :otto,
      version: "0.1.2",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Otto",
      docs: [extras: ["README.md"]],
      description: "A simple wrapper for Aliyun TableStore, based on ex_aliyun_ots",
      source_url: "https://github.com/edragonconnect/otto",
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def package do
    [
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/edragonconnect/otto"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:ex_aliyun_ots, "~> 0.2.2"},
      {:ecto, "~> 3.0"},
      {:jason, "~> 1.1"},
      {:dialyxir, "~> 1.0.0-rc.4", only: [:dev, :test], runtime: false}
    ]
  end
end
