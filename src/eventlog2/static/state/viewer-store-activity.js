(function () {
  const shared = window.EventLog2.viewerStoreShared;

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerActivityState = () => {
    const bookmarkVersion = shared.signalFactory(0);
    const commentVersion = shared.signalFactory(0);

    const bumpBookmarkVersion = () => {
      bookmarkVersion.value = Number(bookmarkVersion.value || 0) + 1;
      return bookmarkVersion.value;
    };

    const bumpCommentVersion = () => {
      commentVersion.value = Number(commentVersion.value || 0) + 1;
      return commentVersion.value;
    };

    return {
      bookmarkVersion,
      commentVersion,
      bumpBookmarkVersion,
      bumpCommentVersion,
    };
  };
})();
