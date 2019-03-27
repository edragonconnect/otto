defmodule Otto.Cipher.AES.GCM do
  @behaviour Otto.Cipher
  @aad "AES256GCM"

  def encrypt(plaintext, iv, key) do
    {ciphertext, ciphertag} = :crypto.block_encrypt(:aes_gcm, key, iv, {@aad, plaintext})
    {:ok, Base.encode64(ciphertag <> ciphertext)}
  end

  def decrypt(ciphertext, iv, key) do
    <<ciphertag::binary-16, ciphertext::binary>> = Base.decode64!(ciphertext)
    {:ok, :crypto.block_decrypt(:aes_gcm, key, iv, {@aad, ciphertext, ciphertag})}
  end
end
