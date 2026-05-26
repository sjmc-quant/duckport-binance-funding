import pathlib
import tomllib
import unittest


class EntrypointTests(unittest.TestCase):
    def test_only_exposes_main_ingestor_script(self):
        pyproject = pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        scripts = data["project"]["scripts"]

        self.assertEqual(scripts, {
            "binance-funding": "binance_funding.main:main",
        })


if __name__ == "__main__":
    unittest.main()
