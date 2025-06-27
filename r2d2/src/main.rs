mod database;
use crate::database::Database;
use bson::spec::ElementType;
use bson::Document;
use chrono::{DateTime, Local};
use std::io::ErrorKind;
use std::io::{BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::{fs, io};

fn create_dir(path: &str) -> Result<(), io::Error> {
    match fs::create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}


fn handle_client(mut stream: TcpStream) {
    println!("Connection from {}", stream.peer_addr().unwrap());
    let buf = &mut [0; 512];

    if let Ok(_) = stream.read(buf) {
        println!("{:?}", buf.to_vec());
    }
}


fn handle_request(_db: &mut Database, _req: String) -> Vec<u8> {
    // Query database and fetch result ////////////////////////
    let result : Document = _db.query(_req.clone());
    ///////////////////////////////////////////////////////////

    let mut html : String = String::from(r#"
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="UTF-8">
                <link rel="icon" href="data:,">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>R2D2</title>

                <style>
                table { border: 2px black solid; width: 80%; }
                th { border: 1px black solid; }
                td { border: 1px black solid; }
                </style>
            </head>
            <body>
                <h1>"#);

    html.push_str(_req.as_str());
    html.push_str( r#"</h1>
        <nav> Quick Actions :
            <a href="/LIST::ALL">List All</a> |
            <a href="/LIST::SAVED">List Saved Databases</a>
        </nav>
    </body>"#);

    if _req.contains("::HIDE") {
        html.push_str("Success");
    }

    else {
        html.push_str(r#"<table>"#);

        // Header
        if let header = result.get_array("labels").unwrap() {
            let mut header_str : Vec<String> = Vec::from(
                header.iter()
                    .filter(|&s| s.element_type() == ElementType::String)
                    .map(|s| {String::from(s.as_str().unwrap())})
                    .collect::<Vec<String>>()
            );

            if (_req.as_str().contains("LIST") || _req.as_str().contains("INSERT"))
                && !_req.as_str().contains("SAVED") {
                header_str.insert(0, String::from("Timestamp"));
            }

            html.push_str(
                &*vec_string_to_html_row(header_str, true)
            );
        }

        // Contents
        if let body = result.get_document("rows").unwrap() {
            for (label, content) in body.iter() {
                match content.element_type() {
                    ElementType::EmbeddedDocument => {
                        html.push_str(
                            &*document_to_html_row(
                                content.as_document().unwrap().clone(),
                                label.parse::<u128>().unwrap_or(0)
                            )
                        );
                    }

                    _ => {
                        html.push_str(format!(
                            "<tr><td>{}</td></tr>",
                            content.to_string()
                        ).as_str());
                    }
                }


            }
        }

        html.push_str( r#"</table>"#);
    }

    html.push_str(r#"</html>"#);

    let mut header = String::new();
    header.push_str("HTTP/1.1 ");
    header.push_str(&200.to_string());
    header.push_str(" OK\r\n\r\n" );

    let mut result:Vec<u8> = Vec::from(header.as_bytes());
    result = [result, Vec::from(html.as_bytes())].concat();

    return result;
}

fn vec_string_to_html_row(v : Vec<String>, header : bool) -> String {
    let mut html = String::from("<tr>");
    for s in v {
        match header {
            true => html.push_str(format!("<th>{}</th>", s).as_str()),
            false => html.push_str(format!("<td>{}</td>", s).as_str()),
        };
    }

    html.push_str("</tr>");

    return html;
}

fn document_to_html_row(doc : Document, time : u128) -> String {
    let mut html = String::from(format!("<tr><td>{}</td>", time).as_str());

    for (_, content) in doc {
        // Don't want label! Ignore it
        let converted = match content.element_type() {
            ElementType::Double => content.as_f64().unwrap().to_string(),
            ElementType::Int32 => content.as_i32().unwrap().to_string(),
            ElementType::Int64 => content.as_i64().unwrap().to_string(),
            ElementType::String => content.as_str().unwrap().to_string(),
            ElementType::Boolean => content.as_bool().unwrap().to_string(),
            _ => String::from("None")
        };

        html.push_str(format!("<td>{}</td>", converted).as_str());
    }

    html.push_str("</tr>");

    return html;
}

fn main() -> std::io::Result<()> {
    if let Err(e) = create_dir(database::DATA_PATH) {
        eprintln!("Error creating directory: {}", e);
    } else {
        println!("Directory created or already exists.");
    }

    let mut database = Database::new(
        vec![
            String::from("store"),
            String::from("product"),
            String::from("number_sold")
        ],
        vec![
            String::from("number"),
            String::from("number"),
            String::from("number")]
    );

    println!("\n\n==========================================================");
    println!("Welcome to R2D2!");
    let current_local: DateTime<Local> = Local::now();
    let custom_format = current_local.format("%Y-%m-%d %H:%M:%S");
    println!("The current time is {}.", custom_format);
    println!("==========================================================\n");


    let listener = TcpListener::bind("127.0.0.1:6969")?;
    println!(">> Listening for requests at http://127.0.0.1:6969/...");

    // accept connections and process them serially
    for mut stream in listener.incoming().flatten() {
        // Inside this loop, someone has connected.

        // You can kind of think of this line of code as if it were
        // a scanner in Java. That's basically what it's doing.
        let mut rdr = io::BufReader::new(&mut stream);

        /* [[[[[[[[[[[[[[ THE LISTEN LOOP ]]]]]]]]]]]]] */
        // This loop will get every string that the listener
        // hears, and print them to the terminal. If it hears
        // an empty line, we break out of the loop.
        let mut i = 0;
        let mut requested_resource: String = String::new();

        loop {
            let mut l = String::new();
            rdr.read_line(&mut l).unwrap();
            if l.trim().is_empty() { break; }

            if i == 0 {
                i = 1;
                requested_resource = l
                    .split(" ").collect::<Vec<&str>>()[1].to_string()
                    .split("/").collect::<Vec<&str>>()[1].to_string();
                if !requested_resource.contains("HIDE") {
                    println!("REQUESTED RESOURCE: {}", requested_resource);
                }
            }
        }

        let response = handle_request(&mut database, requested_resource);
        stream.write(&response).expect("TODO: panic message");

    }

    Ok(())
}