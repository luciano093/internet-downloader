use std::collections::HashSet;

use indexmap::IndexMap;

use crate::download::items::{Download, DownloadId};
use crate::download::verifier::VerifierHandle;

// These two separate sets are needed to track two things:
// is the current download being handled by the verify manager? and
// does the current download already went through the verification process?
// Separating these two allows us to implement pausing verification by just dropping the handle,
// while allowing us to not have to save Verifying as a state to the db, which would be unnecessary
// as no download loaded at the start of the program should be in a Veriying state.
pub struct VerificationTracker {
    verifying: HashSet<DownloadId>,
    needs_verification: HashSet<DownloadId>,
    verifier: VerifierHandle,
}

impl VerificationTracker {
    pub async fn from_downloads(verifier: VerifierHandle, downloads: IndexMap<DownloadId, Download>) -> Self {
        let mut verification_tracker = Self {
            verifying: HashSet::new(),
            needs_verification: HashSet::new(),
            verifier,
        };
        
        for &download_id in downloads.keys() {
            verification_tracker.verifying.insert(download_id);
            verification_tracker.needs_verification.insert(download_id);
        }

        let _ = verification_tracker.verifier.verify_downloads(downloads).await;
        
        verification_tracker
    }

    pub async fn verify(&mut self, download: Download) {
         self.verifying.insert(download.id());
         let _ = self.verifier.verify_download(download).await;
    }

    pub async fn cancel(&mut self, download_id: DownloadId) {
        // Cancel the verification if there is any
        if self.is_verifying(&download_id) {
            let _ = self.verifier.cancel_verification(download_id).await;
        }
    }
    
    pub async fn pause(&mut self, download_id: DownloadId) {
        self.verifying.remove(&download_id);
        let _ = self.verifier.pause_verification(download_id).await;
    }

    pub fn remove(&mut self, download_id: &DownloadId) {
        self.verifying.remove(&download_id);
        self.needs_verification.remove(&download_id);
    }

    pub fn needs_verification(&self, download_id: &DownloadId) -> bool {
        self.needs_verification.contains(&download_id)
    }
     
    pub fn is_verifying(&self, download_id: &DownloadId) -> bool {
         self.verifying.contains(download_id)
    }
}
