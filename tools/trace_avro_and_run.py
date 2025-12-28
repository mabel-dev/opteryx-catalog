# Ensure project is on path
import os
import sys
import traceback

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

try:
    import pyiceberg.avro.file as avf
except Exception as e:
    print("Could not import pyiceberg.avro.file:", e, file=sys.stderr)
    avf = None

if avf is not None:
    _orig = avf.AvroFile.__enter__

    def _patched(self, *a, **kw):
        print("--- AvroFile.__enter__ stack (most recent call last) ---", file=sys.stderr)
        traceback.print_stack(limit=30, file=sys.stderr)
        print("--- end stack ---", file=sys.stderr)
        return _orig(self, *a, **kw)

    avf.AvroFile.__enter__ = _patched

# Now run the target script
script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests", "create_table.py")
with open(script_path, "r") as f:
    code = f.read()

# Execute in fresh globals
_glob = {
    "__name__": "__main__",
    "__file__": script_path,
}
exec(compile(code, script_path, "exec"), _glob)
