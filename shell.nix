{pkgs ? import <nixpkgs> {}}: let
  pythonPackages = pkgs.python3Packages;
  
  pyairtable = pythonPackages.buildPythonPackage rec {
    pname = "pyairtable";
    version = "3.0.1";
    format = "setuptools";
    src = pythonPackages.fetchPypi {
      inherit pname version;
      sha256 = "sha256-cBKiQDujM9uddtk0U63odis9EoGNEc3mcPpstpf0La8=";
    };
    propagatedBuildInputs = with pythonPackages; [requests inflection pydantic];
    doCheck = false;
  };

  hikerapi = pythonPackages.buildPythonPackage rec {
    pname = "hikerapi";
    version = "1.6.3";
    format = "setuptools";
    src = pythonPackages.fetchPypi {
      inherit pname version;
      sha256 = "sha256-Nhedye2U4Oqza4uX084xTqbmrDnjAtSj9aK2qni1Yb8="; # You'll need to update this
    };
    propagatedBuildInputs = with pythonPackages; [requests];
    doCheck = false;
  };

  pythonWithPackages = pkgs.python3.withPackages (ps: with ps; [
    pyyaml
    pyairtable
    google-auth-oauthlib
    google-auth-httplib2
    google-api-python-client
    requests
    python-lsp-server
    python-dotenv
    pandas
    hikerapi
  ]);

in pkgs.mkShell {
  buildInputs = with pkgs; [
    zsh
    pythonWithPackages
  ];

  shellHook = ''
    export ZDOTDIR="$HOME"
    export PYTHONPATH="$PWD:$PYTHONPATH"
    echo "Instagram Analytics Development Environment"
    echo "Python version: $(python --version)"
    echo "Python development environment ready!"
    exec zsh
  '';
}
