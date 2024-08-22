package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.VolumeRepository;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class VolumeService {
  private static final VolumeRepository VOLUME_REPOSITORY = VolumeRepository.getInstance();

  public VolumeService() {}

  @Post("")
  public HttpResponse createVolume(CreateVolumeRequestContent createVolumeRequest) {
    // Throw error if catalog/schema does not exist
    return HttpResponse.ofJson(VOLUME_REPOSITORY.createVolume(createVolumeRequest));
  }

  @Get("")
  public HttpResponse listVolumes(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    return HttpResponse.ofJson(
        VOLUME_REPOSITORY.listVolumes(
            catalogName, schemaName, maxResults, pageToken, includeBrowse));
  }

  @Get("/{full_name}")
  public HttpResponse getVolume(
      @Param("full_name") String fullName,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    return HttpResponse.ofJson(VOLUME_REPOSITORY.getVolume(fullName));
  }

  @Patch("/{full_name}")
  public HttpResponse updateVolume(
      @Param("full_name") String fullName, UpdateVolumeRequestContent updateVolumeRequest) {
    return HttpResponse.ofJson(VOLUME_REPOSITORY.updateVolume(fullName, updateVolumeRequest));
  }

  @Delete("/{full_name}")
  public HttpResponse deleteVolume(@Param("full_name") String fullName) {
    VOLUME_REPOSITORY.deleteVolume(fullName);
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/{full_name}/content")
  public HttpResponse getFileContent() {
    return HttpResponse.of(HttpStatus.OK, MediaType.PLAIN_TEXT_UTF_8, "fileContent");
  }

  @Post("/path")
  public HttpResponse processPath(@RequestObject Map<String, String> request) {

    VolumeInfo volumeInfo = VOLUME_REPOSITORY.getVolume(request.get("volume"));
    String location = volumeInfo.getStorageLocation();
    try {
      URI uri = new URI(location);
      location = uri.getPath();
    } catch (URISyntaxException e) {
      return HttpResponse.ofJson(Map.of("error", "Invalid location URI: " + e.getMessage()));
    }
    String path = request.get("path");
    String fullPath = location + "/" + path;
    File file = new File(fullPath);
    Map<String, Object> response = new HashMap<>();

    if (file.isDirectory()) {
      // List the files and folders in the given path
      File[] filesList = file.listFiles();
      Map<String, String> filesMap = new HashMap<>();
      for (File f : filesList) {
        filesMap.put(f.getName(), f.isDirectory() ? "directory" : "file");
      }
      response.put("files", filesMap);
    } else if (file.isFile()) {
      // Show the content of the file
      try {
        String content = new String(Files.readAllBytes(Paths.get(fullPath)));
        response.put("content", content);
      } catch (IOException e) {
        response.put("error", "Error reading the file: " + e.getMessage());
      }
    } else {
      response.put(
          "error", location + "/" + path + ",The given path is neither a directory nor a file.");
    }

    return HttpResponse.ofJson(response);
  }
}
