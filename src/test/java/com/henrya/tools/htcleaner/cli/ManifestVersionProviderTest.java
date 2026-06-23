package com.henrya.tools.htcleaner.cli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ManifestVersionProviderTest {

  @Test
  @DisplayName("Version provider returns htcleaner version text")
  void versionProviderReturnsHtcleanerVersionText() {
    String[] version = new ManifestVersionProvider().getVersion();

    assertThat(version).hasSize(1);
    assertThat(version[0]).startsWith("htcleaner ");
  }
}
