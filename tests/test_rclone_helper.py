from pathlib import Path

from chelsa_download.rclone_helper import remote_to_http_url


def test_remote_to_http_url_resolves_alias(tmp_path: Path):
    cfg = tmp_path / "rclone.conf"
    cfg.write_text(
        "\n".join(
            [
                "[base]",
                "type = s3",
                "endpoint = example.com",
                "",
                "[alias1]",
                "type = alias",
                "remote = base:bucket/prefix",
                "",
                "[alias2]",
                "type = alias",
                "remote = alias1:subdir",
                "",
            ]
        )
    )

    url = remote_to_http_url("alias2:folder/file.tif", cfg)
    assert url == "https://example.com/bucket/prefix/subdir/folder/file.tif"


def test_remote_to_http_url_fallback_default():
    url = remote_to_http_url("chelsa02_bioclim:bio01/test.tif", None)
    assert url == "https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/bioclim/bio01/test.tif"
