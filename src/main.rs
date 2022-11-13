use cang_jie::CangJieTokenizer;
use cang_jie::TokenizerOption;
use cang_jie::CANG_JIE;
use jieba_rs::Jieba;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::{DefaultOnError, DefaultOnNull};
use std::env;
use std::sync::Arc;
use std::{fs::File, io::BufReader};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;

#[macro_use]
extern crate tantivy;
use tantivy::schema::*;
use tantivy::Index;

#[serde_as]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Item {
    title: String,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    author: String,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    description: String,
    #[serde_as(deserialize_as = "DefaultOnError")]
    year: u64,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    publisher: String,
    #[serde_as(deserialize_as = "DefaultOnError")]
    page: u64,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    language: String,
    filesize: u64,
    extension: String,
    md5: String,
    ipfs_cid: String,
}

impl From<(Schema, Document)> for Item {
    fn from((schema, doc): (Schema, Document)) -> Self {
        macro_rules! get_field_text {
            ($field:expr) => {
                doc.get_first(schema.get_field($field).unwrap())
                    .unwrap()
                    .as_text()
                    .unwrap_or_default()
                    .to_owned()
            };
        }

        macro_rules! get_field_u64 {
            ($field:expr) => {
                doc.get_first(schema.get_field($field).unwrap())
                    .unwrap()
                    .as_u64()
                    .unwrap_or_default()
            };
        }

        Item {
            title: get_field_text!("title"),
            author: get_field_text!("author"),
            // description: get_field_text!("description"),
            description: "".to_string(),
            year: get_field_u64!("year"),
            publisher: get_field_text!("publisher"),
            // page: get_field_u64!("page"),
            page: 0,
            language: get_field_text!("language"),
            // filesize: get_field_u64!("filesize"),
            filesize: 0,
            extension: get_field_text!("extension"),
            // md5: get_field_text!("md5"),
            md5: "".to_string(),
            ipfs_cid: get_field_text!("ipfs_cid"),
        }
    }
}

fn index() {
    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer(CANG_JIE)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();

    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", text_options.clone());
    // let description = schema_builder.add_text_field("description", text_options.clone());
    let author = schema_builder.add_text_field("author", text_options.clone());
    let publisher = schema_builder.add_text_field("publisher", text_options.clone());
    let language = schema_builder.add_text_field("language", TEXT | STORED);
    let year = schema_builder.add_u64_field("year", STORED);
    // let page = schema_builder.add_u64_field("page", STORED);
    // let filesize = schema_builder.add_u64_field("filesize", STORED);
    let extension = schema_builder.add_text_field("extension", STORED);
    // let md5 = schema_builder.add_text_field("md5", STORED);
    let ipfs_cid = schema_builder.add_text_field("ipfs_cid", STORED);
    let schema = schema_builder.build();

    // index
    let index = Index::create_in_dir("index", schema.clone()).unwrap();

    let tokenizer = CangJieTokenizer {
        worker: Arc::new(Jieba::new()),
        option: TokenizerOption::Unicode,
    };
    index.tokenizers().register(CANG_JIE, tokenizer);

    let mut writer = index.writer(10 * 1024 * 1024 * 1024).unwrap();

    let file = File::open("../search.csv").unwrap();
    let reader = BufReader::new(file);

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader);

    for result in rdr.deserialize::<Item>() {
        match result {
            Ok(item) => {
                if let Err(err) = writer.add_document(doc!(
                    title => item.title,
                    // description => item.description,
                    author => item.author,
                    publisher => item.publisher,
                    language => item.language,
                    year => item.year,
                    // page => item.page,
                    // filesize => item.filesize,
                    extension => item.extension,
                    // md5 => item.md5,
                    ipfs_cid => item.ipfs_cid,
                )) {
                    println!("{err}");
                }
            }
            Err(err) => {
                println!("{err}");
            }
        }
    }

    writer.commit().unwrap();
}

fn main() {
    // index();

    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer(CANG_JIE)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();

    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", text_options.clone());
    // let description = schema_builder.add_text_field("description", text_options.clone());
    let author = schema_builder.add_text_field("author", text_options.clone());
    let publisher = schema_builder.add_text_field("publisher", text_options.clone());
    let language = schema_builder.add_text_field("language", text_options.clone());
    let year = schema_builder.add_u64_field("year", STORED);
    // let page = schema_builder.add_u64_field("page", STORED);
    // let filesize = schema_builder.add_u64_field("filesize", STORED);
    let extension = schema_builder.add_text_field("extension", STRING | STORED);
    // let md5 = schema_builder.add_text_field("md5", STRING | STORED);
    let ipfs_cid = schema_builder.add_text_field("ipfs_cid", STRING | STORED);
    let schema = schema_builder.build();

    // search
    let query: Vec<String> = env::args().collect();
    if query.len() < 2 {
        println!("search [BOOK | AUTHOR]");
        return;
    }

    let index = Index::open_in_dir("index").unwrap();
    let tokenizer = CangJieTokenizer {
        worker: Arc::new(Jieba::new()),
        option: TokenizerOption::Unicode,
    };
    index.tokenizers().register(CANG_JIE, tokenizer);

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title, author]);

    let query = query_parser.parse_query(query[1].as_str()).unwrap();

    let top_docs = searcher.search(&query, &TopDocs::with_limit(50)).unwrap();

    for (_, doc_address) in top_docs {
        let doc = searcher.doc(doc_address).unwrap();
        let item: Item = (schema.clone(), doc).into();
        println!(
            "/ipfs/{}\t{}\t{}\t{}\t [{}] <{}> {{{}}}",
            item.ipfs_cid,
            item.extension,
            item.language,
            item.year,
            item.title,
            item.author,
            item.publisher,
        );
    }
}

#[test]
fn test_csv_der() {
    let file = File::open("../search.csv").unwrap();
    let reader = BufReader::new(file);

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader);
    for result in rdr.records() {
        if let Err(err) = result {
            println!("{err:?}");
            break;
        }
    }
    println!("{:?}", rdr.position());
}
