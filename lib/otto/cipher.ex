defmodule Otto.Cipher do
  @moduledoc """
  It is a behaviour for encryption/decryption modules. If you want to write your own
  cipher module, you should implement this behaviour.
  """

  @type plaintext :: String.t()
  @type ciphertext :: binary()
  @type key :: binary()
  @type iv :: binary()

  @callback encrypt(plaintext, iv, key) :: {:ok, ciphertext} | :error

  @callback decrypt(ciphertext, iv, key) :: {:ok, plaintext} | :error

  @spec encrypt(plaintext, iv, String.t() | nil) :: {:ok, ciphertext}
  def encrypt(plaintext, iv, tag \\ nil) do
    {module, key} = config(tag)
    module.encrypt(plaintext, iv, key)
  end

  @spec decrypt(nil, iv, String.t()) :: {:ok, nil}
  def decrypt(nil, _iv, _tag), do: {:ok, nil}

  @spec decrypt(ciphertext, iv, String.t()) :: {:ok, plaintext}
  def decrypt(ciphertext, iv, tag) do
    {module, key} = config(tag)
    module.decrypt(ciphertext, iv, key)
  end

  # Utils

  @doc """
  Generate a cipher key for your cipher config.
  """
  def generate_key, do: :crypto.strong_rand_bytes(32) |> Base.encode64()

  def generate_iv, do: :crypto.strong_rand_bytes(16)

  def config(tag \\ nil)

  def config(tag) when is_binary(tag) do
    String.to_atom(tag) |> config
  end

  def config(tag) do
    config = Application.get_env(:otto, :ciphers)

    cipher =
      if tag do
        config[tag]
      else
        hd(config) |> elem(1)
      end

    {Keyword.fetch!(cipher, :module), Keyword.fetch!(cipher, :key)}
  end

  def active_tag do
    Application.get_env(:otto, :ciphers) |> List.first() |> elem(0)
  end
end
