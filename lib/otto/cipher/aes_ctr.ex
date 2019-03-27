defmodule Otto.Cipher.AES.CTR do
  @behaviour Otto.Cipher

  def encrypt(plaintext, iv, key) do
    state = :crypto.stream_init(:aes_ctr, key, iv)
    {_state, ciphertext} = :crypto.stream_encrypt(state, plaintext)
    {:ok, Base.encode64(ciphertext)}
  end

  def decrypt(ciphertext, iv, key) do
    ciphertext = Base.decode64!(ciphertext)
    state = :crypto.stream_init(:aes_ctr, key, iv)
    {_state, plaintext} = :crypto.stream_decrypt(state, ciphertext)
    {:ok, plaintext}
  end
end
