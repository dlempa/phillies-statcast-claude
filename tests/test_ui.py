from __future__ import annotations

import unittest

from support import PROJECT_ROOT  # noqa: F401

from phillies_stats.ui import _clean_state_summary_text


class UiTests(unittest.TestCase):
    def test_state_summary_text_restores_apostrophes_from_question_marks(self):
        text = "Bryce Harper?s night and Zack Wheeler?s return. Is this real?"

        cleaned = _clean_state_summary_text(text)

        self.assertEqual(cleaned, "Bryce Harper's night and Zack Wheeler's return. Is this real?")


if __name__ == "__main__":
    unittest.main()
