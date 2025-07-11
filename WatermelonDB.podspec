require "json"

package = JSON.parse(File.read("package.json"))

Pod::Spec.new do |s|
  s.name         = "WatermelonDB"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.description  = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.author       = { "author" => package["author"] }
  s.platforms    = { :ios => "9.0", :tvos => "9.0" }
  s.source       = { :http => 'file:' + __dir__ + '/'  }
  s.source       = { :git => "https://github.com/BuildHero/WatermelonDB", :tag => "v#{s.version}" }
  s.source_files = "native/ios/**/*.{h,m,mm,swift,c,cpp}", "native/shared/*.{h,c,cpp}"
  s.public_header_files = '**/Bridging.h'
  s.requires_arc = true
  s.dependency "React"
  s.dependency "React-jsi"

  s.pod_target_xcconfig = {
    "HEADER_SEARCH_PATHS" => [
      "${PODS_ROOT}/RCT-Folly",
    ],
    "GCC_PREPROCESSOR_DEFINITIONS" => "$(inherited) FOLLY_NO_CONFIG FOLLY_CFG_NO_COROUTINES",
    "OTHER_CPLUSPLUSFLAGS" => "-DFOLLY_NO_CONFIG -DFOLLY_MOBILE=1 -DFOLLY_USE_LIBCPP=1"
  }
end
