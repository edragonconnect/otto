defmodule OttoTest.CipherTest do
  use ExUnit.Case
  alias Otto.Cipher

  test "check invalid cipher config" do
    fake_config = [
      aes_gcm_v2: [
        module: Otto.Cipher.AES.GCM,
        key: "2DR+mrNKNv3bGsQA2VnvTy8WrUwtNiO28/VXgWwAYEE="
      ],
      aes_gcm_v1: [
        module: Otto.Cipher.AES.GCM,
        key: "QLHEOuMbWAQVkfe3u14gNOZYajKOgz0q0mB7cyjdBTo"
      ]
    ]
  end

  test "encrypt and decrypt" do
    tag = "aes_gcm_v2"
    iv = Cipher.generate_iv()
    plaintext = "Hello Elixir"
    assert {:ok, ciphertext} = Cipher.encrypt(plaintext, iv)
    assert {:ok, plaintext} = Cipher.decrypt(ciphertext, iv, tag)
  end
end
