# Changelog

All notable changes to this project will be documented here.

## [Unreleased]
- (add items that are in-progress)

## [2025-09-16]
### Changed
- **Classifier**: Added pitch-template override that flips release-boilerplate Pitch files to pitch_email and lowers confidence to 0.60 for manual review.
- **Locking**: set_label now respects existing _locked decisions to prevent later overrides.
- **Constraints**: enforce_extract_constraints prefers media_list when filename or schema indicates so; filename passed to function.
- **Rules**: Filename "release" now locks press_release.

### Verified
- 211_LA: Frontlines Pitch  pitch_email (0.60), Release_Data Dashboard  press_release, Interview Guides  interview_guide, Media Lists  media_list, Timeline  	imeline.

## [2025-09-21]
### Added
- Triage workflow (	riage_needs_review.csv) with proposed label, uncertainty, evidence, reasons.
- Resume-safe classification (skip by sha256).
- Unknown label guard: \unmapped_other\.
- Embedding cost flag in README_DEV.md.

### Changed
- SYSTEM_PROMPT: allow \proposed_new_label\, optional \uncertainty_reason\.
- coerce_to_allowed: preserve \_model_doc_type\, set \_coerced\, bound confidence on coercions.
- soft_override: filename 'release' lock; pitch template override  \pitch_email\ @ 0.60; lock semantics.
- set_label: now respects existing \_locked\.
- enforce_extract_constraints: prefer \media_list\; add \_extract_constraint_adjusted_from\; cap conf to 0.85.

### Added (scripts/pipeline)
- \scripts/process_all_campaigns.py\
- \pipeline/chunk.py\

### Verified
- 211 LA: Frontlines Pitch  \pitch_email (0.60)\; Release_Data Dashboard  \press_release\; guides/lists/timeline correct.
