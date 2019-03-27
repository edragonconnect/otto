defmodule OttoTest.AESCTRTest do
  use ExUnit.Case
  alias Otto.Cipher

  @key Cipher.generate_key() |> Base.decode64!()
  @iv Cipher.generate_iv()
  @plaintext "Hello Elixir"

  test "aes gcm encryption and decryption" do
    encrypt_result = Cipher.AES.CTR.encrypt(@plaintext, @iv, @key)
    assert {:ok, ciphertext} = encrypt_result
    assert {:ok, binary_ciphertext} = Base.decode64(ciphertext)
    assert String.length(binary_ciphertext) > 0

    assert {:ok, plaintext} = Cipher.AES.CTR.decrypt(ciphertext, @iv, @key)
    assert plaintext == @plaintext
  end
end
