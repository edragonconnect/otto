defmodule Otto.Row do
  alias Otto.Cipher

  @tag_key "__aes_tag__"
  @iv_key "__aes_iv__"

  def encrypt(attrs, fields, get_cipher_fn \\ &new_cipher/0)

  def encrypt(attrs, nil, _), do: attrs

  def encrypt(attrs, fields, get_cipher_fn) do
    {:ok, iv, tag} = get_cipher_fn.()

    attrs =
      Enum.map(attrs, fn {key, value} ->
        if String.to_atom(key) in fields do
          {:ok, ciphertext} = Cipher.encrypt(to_string(value), iv, tag)
          {key, ciphertext}
        else
          {key, value}
        end
      end)

    [{@tag_key, to_string(tag)}, {@iv_key, Base.encode64(iv)} | attrs]
  end

  def decrypt(attrs, nil), do: attrs

  def decrypt(attrs, fields) do
    {:ok, iv, tag} = fetch_cipher(attrs)

    Enum.map(attrs, fn
      {key, value, time} ->
        if String.to_atom(key) in fields do
          {:ok, plaintext} = Cipher.decrypt(value, iv, tag)
          {key, plaintext, time}
        else
          {key, value, time}
        end
    end)
  end

  def new_cipher do
    iv = Cipher.generate_iv()
    tag = Cipher.active_tag()
    {:ok, iv, tag}
  end

  def fetch_cipher(columns) when is_list(columns) do
    {_, iv, _} = List.keyfind(columns, @iv_key, 0)
    {_, tag, _} = List.keyfind(columns, @tag_key, 0)

    {:ok, Base.decode64!(iv), tag}
  end

  def cipher_columns, do: [@tag_key, @iv_key]
end
