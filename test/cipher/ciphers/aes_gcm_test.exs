defmodule OttoTest.AESGCMTest do
  use ExUnit.Case
  alias Otto.Cipher

  @key Cipher.generate_key() |> Base.decode64!()
  @iv Cipher.generate_iv()
  @plaintext "Hello Elixir"

  test "aes gcm encryption and decryption" do
    encrypt_result = Cipher.AES.GCM.encrypt(@plaintext, @iv, @key)
    assert {:ok, ciphertext} = encrypt_result
    assert {:ok, binary_ciphertext} = Base.decode64(ciphertext)
    assert <<bi_ciphertag::binary-16, bi_ciphertext::binary>> = binary_ciphertext
    assert byte_size(bi_ciphertag) == 16
    assert String.length(bi_ciphertext) > 0

    assert {:ok, plaintext} = Cipher.AES.GCM.decrypt(ciphertext, @iv, @key)
    assert plaintext == @plaintext
  end
end
