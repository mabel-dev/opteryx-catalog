import logging
import traceback

try:
    import pyiceberg.avro.file as avf
except Exception:
    avf = None

if avf is not None:
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    _orig_enter = avf.AvroFile.__enter__

    def _patched_enter(self, *a, **kw):
        logging.warning(
            "AvroFile.__enter__ called; stack:\n%s", "".join(traceback.format_stack(limit=30))
        )
        return _orig_enter(self, *a, **kw)

    avf.AvroFile.__enter__ = _patched_enter
