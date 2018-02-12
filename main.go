package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"upspin.io/bind"
	"upspin.io/client"
	"upspin.io/config"
	"upspin.io/errors"
	"upspin.io/flags"
	"upspin.io/log"
	"upspin.io/pack"
	"upspin.io/path"
	_ "upspin.io/transports"
	"upspin.io/upspin"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	drive "google.golang.org/api/drive/v3"
)

const dirMimeType = "application/vnd.google-apps.folder"

func main() {
	flags.Parse(flags.Client)
	cfg, err := config.FromFile(flags.Config)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	conf := oauth2.Config{
		ClientID:     "237409120595-evjrpl3bt06uko30uldbr0ms9agfdotg.apps.googleusercontent.com",
		ClientSecret: "aaofkFRU8rt-r5Bqc9BHHPcB",
		Endpoint:     google.Endpoint,
		Scopes:       []string{drive.DriveScope},
		RedirectURL:  "oob",
	}

	var tok *oauth2.Token
	cacheFile := filepath.Join(os.Getenv("HOME"), ".drive2upspin-token")
	b, err := ioutil.ReadFile(cacheFile)
	if err == nil {
		err = json.Unmarshal(b, &tok)
	} else if os.IsNotExist(err) {
		url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline)
		fmt.Printf("Visit the URL for the auth dialog: %v\n\nType auth code: ", url)

		var code string
		_, err = fmt.Scan(&code)
		if err != nil {
			log.Fatal(err)
		}

		httpClient := &http.Client{Timeout: 5 * time.Second}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

		tok, err = conf.Exchange(ctx, code)
		if err != nil {
			log.Fatal(err)
		}

		b, err = json.Marshal(tok)
		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(cacheFile, b, 0600)
	}
	if err != nil {
		log.Fatal(err)
	}

	oauthClient := conf.Client(ctx, tok)
	svc, err := drive.New(oauthClient)
	if err != nil {
		log.Fatal(err)
	}

	const dirName = "photos"

	q := fmt.Sprintf("name = '%s' and mimeType = '%s'", dirName, dirMimeType)
	list, err := svc.Files.List().Q(q).Do()
	if err != nil {
		log.Fatal(err)
	}
	if len(list.Files) != 1 {
		log.Fatalf("found %d files looking for %q directory", len(list.Files), dirName)
	}
	dir := list.Files[0]

	cp := &copier{
		ctx:      ctx,
		cfg:      cfg,
		upspin:   client.New(cfg),
		drive:    svc,
		doneFile: "drive2upspin-done.txt",
		doneDirs: make(map[string]bool),
	}

	f, err := os.Open(cp.doneFile)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	if err == nil {
		s := bufio.NewScanner(f)
		for s.Scan() {
			if s.Text() != "" {
				cp.doneDirs[s.Text()] = true
			}
		}
	}
	f.Close()

	err = cp.copy(dir, path.Join(upspin.PathName(cfg.UserName()), dirName))
	if err != nil {
		log.Fatal(err)
	}
}

type copier struct {
	ctx      context.Context
	cfg      upspin.Config
	upspin   upspin.Client
	drive    *drive.Service
	doneFile string
	doneDirs map[string]bool
}

func (cp *copier) copy(file *drive.File, dst upspin.PathName) error {
	if file.MimeType == dirMimeType {
		if cp.doneDirs[file.Id] {
			log.Debug.Printf("skipping done directory: %s", dst)
			return nil
		}

		doGlob := true // Assume directory exists and has partial/full content.
		de, err := cp.upspin.Lookup(dst, false)
		if err == nil && !de.IsDir() {
			return errors.E(dst, errors.NotDir)
		}
		if errors.Is(errors.NotExist, err) {
			doGlob = false
			_, err = cp.upspin.MakeDirectory(dst)
		}
		if err != nil {
			return err
		}

		sizes := make(map[string]int64) // [base file name]size
		if doGlob {
			des, err := cp.upspin.Glob(upspin.AllFilesGlob(dst))
			if err != nil {
				return err
			}
			for _, de := range des {
				if !de.IsRegular() {
					continue
				}
				size, err := de.Size()
				if err != nil {
					return err
				}
				p, _ := path.Parse(de.Name)
				sizes[p.Elem(p.NElem()-1)] = size
			}
		}

		var files []*drive.File
		q := fmt.Sprintf("'%s' in parents", file.Id)
		err = cp.drive.Files.List().Fields("files(id)", "files(name)", "files(size)", "files(mimeType)").Q(q).
			Pages(cp.ctx, func(list *drive.FileList) error {
				files = append(files, list.Files...)
				return nil
			})
		if err != nil {
			return err
		}
		for _, f := range files {
			dst2 := path.Join(dst, f.Name)
			if size, ok := sizes[f.Name]; ok {
				if size != f.Size {
					return errors.E(dst2, fmt.Sprintf("upspin file exists with size %d, source file size %d", size, f.Size))
				}
				if f.MimeType == dirMimeType {
					return errors.E(dst2, "upspin file exists, but source is a directory")
				}
				log.Debug.Printf("skipping existing file: %s", dst2)
				continue
			}
			if err := cp.copy(f, dst2); err != nil {
				return err
			}
		}

		cp.doneDirs[file.Id] = true

		f, err := os.OpenFile(cp.doneFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		_, err = f.Seek(0, os.SEEK_END)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(f, file.Id)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}

		return nil
	}

	log.Info.Printf("copying: %s (%d bytes)", dst, file.Size)

	resp, err := cp.drive.Files.Get(file.Id).Download()
	if err != nil {
		return errors.E(dst, err)
	}
	defer resp.Body.Close()
	f, err := cp.upspin.Create(dst)
	if err != nil {
		return err
	}
	err = upspinPut(cp.cfg, dst, resp.Body)
	if err != nil {
		f.Close()
		cp.upspin.Delete(dst)
		return err
	}

	return nil
}

func dump(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "  ")
	log.Printf("%s", b)
}

func upspinPut(cfg upspin.Config, name upspin.PathName, r io.Reader) error {
	parsed, err := path.Parse(name)
	if err != nil {
		return err
	}

	readers := []upspin.UserName{parsed.User()}

	// Encrypt data according to the preferred packer
	packer := pack.Lookup(cfg.Packing())
	if packer == nil {
		return errors.E(name, errors.Errorf("unrecognized Packing %d", cfg.Packing()))
	}

	entry := &upspin.DirEntry{
		Name:       name,
		SignedName: name,
		Packing:    packer.Packing(),
		Time:       upspin.Now(),
		Sequence:   upspin.SeqIgnore,
		Writer:     cfg.UserName(),
		Link:       "",
		Attr:       upspin.AttrNone,
	}

	// Start the I/O.
	store, err := bind.StoreServer(cfg, cfg.StoreEndpoint())
	if err != nil {
		return err
	}
	bp, err := packer.Pack(cfg, entry)
	if err != nil {
		return err
	}
	buf := make([]byte, flags.BlockSize)
	for {
		data := buf
		n, err := io.ReadFull(r, data)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		data = data[:n]

		cipher, err := bp.Pack(data[:n])
		if err != nil {
			return err
		}
		refdata, err := store.Put(cipher)
		if err != nil {
			return err
		}
		bp.SetLocation(
			upspin.Location{
				Endpoint:  cfg.StoreEndpoint(),
				Reference: refdata.Reference,
			},
		)
	}
	if err := bp.Close(); err != nil {
		return err
	}

	// Add other readers to Packdata.
	readersPublicKey := make([]upspin.PublicKey, 0, len(readers)+2)
	f := cfg.Factotum()
	if f == nil {
		return errors.E(name, errors.Permission, "no factotum available")
	}
	for _, r := range readers {
		key, err := bind.KeyServer(cfg, cfg.KeyEndpoint())
		if err != nil {
			return err
		}
		u, err := key.Lookup(r)
		if err != nil || len(u.PublicKey) == 0 {
			// TODO warn that we can't process one of the readers?
			continue
		}
		readersPublicKey = append(readersPublicKey, u.PublicKey)
	}

	packdata := make([]*[]byte, 1)
	packdata[0] = &entry.Packdata
	packer.Share(cfg, readersPublicKey, packdata)

	dir, err := client.New(cfg).DirServer(name)
	if err != nil {
		return err
	}

	_, err = dir.Put(entry)
	return err
}
