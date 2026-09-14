package materialize

// ApplyMergePatch applies an RFC 7396 JSON merge patch to target, returning
// the patched value without modifying target.
func ApplyMergePatch(target, patch any) any {
	patchObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}

	targetObj, ok := target.(map[string]any)
	if !ok {
		targetObj = nil // a non-object target is replaced by the patched object
	}

	out := make(map[string]any, len(targetObj)+len(patchObj))
	for k, v := range targetObj {
		out[k] = v
	}
	for k, v := range patchObj {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = ApplyMergePatch(out[k], v)
		}
	}
	return out
}
