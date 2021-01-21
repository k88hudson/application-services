/* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

pub mod address;
// pub mod credit_card;

use crate::error::Result;
use interrupt_support::Interruptee;
use rusqlite::Connection;
use sync_guid::Guid as SyncGuid;

// Some enums that help represent what the state of local records are.
// The idea is that the actual implementations just need to tell you what
// exists and what doesn't, but don't need to implement the actual policy for
// what that means.

// An "incoming" record can be in only 2 states.
enum IncomingRecordInfo<T> {
    Record { record: T },
    Tombstone { guid: SyncGuid },
}

// A local record can be in any of these 4 states.
enum LocalRecordInfo<T> {
    Unmodified { record: T },
    Modified { record: T },
    Tombstone { guid: SyncGuid },
    Missing,
}

// An enum for the return value from our "merge" function, which might either
// update the record, or might fork it.
enum MergeResult<T> {
    Merged { merged: T},
    Forked { forked: T }
}

// This ties the 3 possible records together and is what we expect the
// implementations to put together for us.
pub struct IncomingState<T> {
    incoming: IncomingRecordInfo<T>,
    local: LocalRecordInfo<T>,
    // We don't have an enum for the mirror - an Option<> is fine because we
    // don't store tombstones there.
    mirror: Option<T>,
}

/// Convert a IncomingState to an IncomingAction - this is where the "policy"
/// lives for when we resurrect, or merge etc.
fn plan_incoming<T>(
    conn: &Connection,
    rec_impl: &dyn RecordImpl<Record = T>,
    staged_info: IncomingState<T>,
) -> Result<IncomingAction<T>> {
    let IncomingState {
        incoming,
        local,
        mirror,
    } = staged_info;

    let state = match incoming {
        IncomingRecordInfo::Tombstone { guid } => {
            match local {
                LocalRecordInfo::Unmodified { .. } => {
                    // Note: On desktop, when there's a local record for an incoming tombstone, a local tombstone
                    // would created. But we don't actually need to create a local tombstone here. If we did it would
                    // immediately be deleted after being uploaded to the server.
                    IncomingAction::DeleteLocalRecord { guid }
                }
                LocalRecordInfo::Modified { record } => {
                    // Incoming tombstone with local changes should cause us to "resurrect" the local.
                    // At a minimum, the implementation will need to ensure the record is marked as
                    // dirty so it's uploaded, overwriting the server's tombstone.
                    IncomingAction::ResurrectRemoteTombstone { record }
                }
                LocalRecordInfo::Tombstone {
                    guid: tombstone_guid,
                } => {
                    assert_eq!(guid, tombstone_guid);
                    IncomingAction::DoNothing
                }
                LocalRecordInfo::Missing => IncomingAction::DoNothing,
            }
        }
        IncomingRecordInfo::Record {
            record: mut incoming_record,
        } => {
            match local {
                LocalRecordInfo::Unmodified { record: local_record } => {
                    // We still need to merge the metadata, but we don't reupload
                    // just for metadata changes, so don't flag the local item
                    // as dirty.
                    rec_impl.merge_metadata(&mut incoming_record, &local_record, &mirror);
                    IncomingAction::Update { record: incoming_record, was_merged: false }
                }
                LocalRecordInfo::Modified {
                    record: local_record,
                } => {
                    match rec_impl.merge(&incoming_record, &local_record, &mirror) {
                        MergeResult::Merged { merged } => {
                            // The record we save locally has material differences
                            // from the incoming one, so we are going to need to
                            // reupload it.
                            IncomingAction::Update { record: merged, was_merged: true }
                        },
                        MergeResult::Forked { forked } => IncomingAction::Fork { forked, incoming: incoming_record }
                    }
                }
                LocalRecordInfo::Tombstone { .. } => IncomingAction::ResurrectLocalTombstone {
                    record: incoming_record,
                },
                LocalRecordInfo::Missing => {
                    match rec_impl.get_local_dupe(conn, &incoming_record)? {
                        None => IncomingAction::Insert {
                            record: incoming_record,
                        },
                        Some((old_guid, local_dupe)) => {
                            // *sob* - need guid fetching in the trait??? assert_ne!(incoming_record.guid, local_dupe.guid);
                            // The existing item is identical except for the metadata, so
                            // we still merge that metadata.
                            rec_impl.merge_metadata(&mut incoming_record, &local_dupe, &mirror);
                            IncomingAction::UpdateLocalGuid {
                                old_guid,
                                record: incoming_record,
                            }
                        }
                    }
                }
            }
        }
    };
    Ok(state)
}

/// The distinct incoming sync actions to be performed for incoming records.
#[derive(Debug, PartialEq)]
pub enum IncomingAction<T> {
    // Remove the local record with this GUID.
    DeleteLocalRecord { guid: SyncGuid },
    // Insert a new record.
    Insert { record: T },
    // Update an existing record. If `was_merged` was true, then the updated
    // record isn't identical to the incoming one, so needs to be flagged as
    // dirty.
    Update { record: T, was_merged: bool },
    // We forked a record because we couldn't merge it. `forked` will have
    // a new guid, while `incoming` is the unmodified version of the incoming
    // record which we need to apply.
    Fork { forked: T, incoming: T },
    // An existing record with old_guid needs to be replaced with this record.
    UpdateLocalGuid { old_guid: SyncGuid, record: T },
    // There's a remote tombstone, but our copy of the record is dirty. The
    // remote tombstone should be replaced with this.
    ResurrectRemoteTombstone { record: T },
    // There's a local tombstone - it should be removed and replaced with this.
    ResurrectLocalTombstone { record: T },
    // Nothing to do.
    DoNothing,
}

// A trait that abstracts the implementation of the specific record types, and
// must be implemented by the concrete record owners.
trait RecordImpl {
    type Record;

    fn fetch_incoming_states(&self, conn: &Connection) -> Result<Vec<IncomingState<Self::Record>>>;

    // Merge or fork multiple records into 1. The resulting record might have
    // the same guid as the inputs, meaning it was truly merged, or a different
    // guid, in which case it was forked.
    fn merge(
        &self,
        incoming: &Self::Record,
        local: &Self::Record,
        mirror: &Option<Self::Record>,
    ) -> MergeResult<Self::Record>;

    // Merge the metadata of 3 records.
    fn merge_metadata(
        &self,
        result: &mut Self::Record,
        other: &Self::Record,
        mirror: &Option<Self::Record>,
    );

    /// Returns a local record that has the same values as the given incoming record (with the exception
    /// of the `guid` values which should differ) that will be used as a local duplicate record for
    /// syncing.
    fn get_local_dupe(
        &self,
        conn: &Connection,
        incoming: &Self::Record,
    ) -> Result<Option<(SyncGuid, Self::Record)>>;

    // Apply a specific action
    fn apply_action(&self, conn: &Connection, action: IncomingAction<Self::Record>) -> Result<()>;

    // Will need new stuff for, "finish incoming" and all outgoing.
}

// needs a better name :) But this is how all the above ties together.
#[allow(dead_code)]
fn do_incoming<T>(
    conn: &Connection,
    rec_impl: &dyn RecordImpl<Record = T>,
    signal: &dyn Interruptee,
) -> Result<()> {
    let states = rec_impl.fetch_incoming_states(conn)?;
    for state in states {
        signal.err_if_interrupted()?;
        let action = plan_incoming(conn, rec_impl, state)?;
        rec_impl.apply_action(conn, action)?;
    }
    Ok(())
}

// Helpers for tests
#[cfg(test)]
pub mod test {
    use crate::db::{schema::create_empty_sync_temp_tables, test::new_mem_db, AutofillDb};

    pub fn new_syncable_mem_db() -> AutofillDb {
        let _ = env_logger::try_init();
        let db = new_mem_db();
        create_empty_sync_temp_tables(&db).expect("should work");
        db
    }
}

#[cfg(test)]
mod tests {
    use super::test::new_syncable_mem_db;
    use super::*;

    struct TestImpl {}

    impl RecordImpl for TestImpl {
        type Record = i32;

        fn fetch_incoming_states(
            &self,
            _conn: &Connection,
        ) -> Result<Vec<IncomingState<Self::Record>>> {
            unreachable!();
        }

        fn merge(
            &self,
            incoming: &Self::Record,
            local: &Self::Record,
            _mirror: &Option<Self::Record>,
        ) -> MergeResult<Self::Record> {
            // If the records are actually identical, or even, we say we merged.
            if incoming == local {
                MergeResult::Merged { merged: *incoming }
            } else {
                MergeResult::Forked { forked: incoming + local }
            }
        }

        fn merge_metadata(
            &self,
            _result: &mut Self::Record,
            _other: &Self::Record,
            _mirror: &Option<Self::Record>,
        ) {
           // do nothing.
        }

        fn get_local_dupe(
            &self,
            _conn: &Connection,
            incoming: &Self::Record,
        ) -> Result<Option<(SyncGuid, Self::Record)>> {
            // For the sake of this test, we pretend even numbers have a dupe.
            Ok(if incoming % 2 == 0 {
                Some((SyncGuid::random(), *incoming))
            } else {
                None
            })
        }

        // Apply a specific action
        fn apply_action(
            &self,
            _conn: &Connection,
            _action: IncomingAction<Self::Record>,
        ) -> Result<()> {
            unreachable!();
        }
    }

    #[test]
    fn test_plan_incoming_record() -> Result<()> {
        let conn = new_syncable_mem_db();
        let testimpl = TestImpl {};
        let guid = SyncGuid::random();
        // We just use an int for <T> here, hence the magic 0

        // LocalRecordInfo::UnModified - update the local with the incoming.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 0 },
            local: LocalRecordInfo::Unmodified { record: 1 },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::Update { record: 0, was_merged: false }
        );

        // LocalRecordInfo::Modified - but it turns out they are identical.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 0 },
            local: LocalRecordInfo::Modified { record: 0 },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::Update { record: 0, was_merged: true }
        );

        // LocalRecordInfo::Modified and they need to be "forked"
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 1 },
            local: LocalRecordInfo::Modified { record: 2 },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::Fork { forked: 3, incoming: 1 }
        );

        // LocalRecordInfo::Tombstone - the local tombstone needs to be
        // resurrected.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 1 },
            local: LocalRecordInfo::Tombstone { guid: guid.clone() },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::ResurrectLocalTombstone { record: 1 }
        );

        // LocalRecordInfo::Missing and a local dupe (even numbers will dupe)
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 0 },
            local: LocalRecordInfo::Missing,
            mirror: None,
        };
        assert!(
            matches!(
                plan_incoming(&conn, &testimpl, state)?,
                IncomingAction::UpdateLocalGuid { record: 0, .. }
            )
        );

        // LocalRecordInfo::Missing and no dupe - it's an insert.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Record { record: 1 },
            local: LocalRecordInfo::Missing,
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::Insert { record: 1 }
        );
        Ok(())
    }

    #[test]
    fn test_plan_incoming_tombstone() -> Result<()> {
        let conn = new_syncable_mem_db();
        let testimpl = TestImpl {};
        let guid = SyncGuid::random();
        // We just use an int for <T> here, hence the magic 0

        // LocalRecordInfo::Modified
        // Incoming tombstone with an modified local record deletes the local record.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Tombstone { guid: guid.clone() },
            local: LocalRecordInfo::Unmodified { record: 0 },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::DeleteLocalRecord { guid: guid.clone() }
        );

        // LocalRecordInfo::Modified
        // Incoming tombstone with an modified local record keeps the local record.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Tombstone { guid: guid.clone() },
            local: LocalRecordInfo::Modified { record: 0 },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::ResurrectRemoteTombstone { record: 0 }
        );

        // LocalRecordInfo::Tombstone
        // Local tombstone and incoming tombstone == DoNothing.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Tombstone { guid: guid.clone() },
            local: LocalRecordInfo::Tombstone { guid: guid.clone() },
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::DoNothing
        );

        // LocalRecordInfo::Missing
        // Incoming tombstone and no local record == DoNothing.
        let state = IncomingState {
            incoming: IncomingRecordInfo::Tombstone { guid: guid.clone() },
            local: LocalRecordInfo::Missing,
            mirror: None,
        };
        assert_eq!(
            plan_incoming(&conn, &testimpl, state)?,
            IncomingAction::DoNothing
        );
        Ok(())
    }

}
