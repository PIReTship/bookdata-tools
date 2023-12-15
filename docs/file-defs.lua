function rust_parse_ref(name)
    local tpath, tname, label
    tpath, tname = string.match(name, "^~(.*::([^:]+))")
    if tpath then
        label = pandoc.Code(tname)
    else
        tpath = name
        label = pandoc.Code(name)
    end
    return tpath, label
end

function rust_link(arg, type)
    local name, path, label, tgt
    name = pandoc.utils.stringify(arg)
    path, label = rust_parse_ref(name)
    tgt = "/apidocs/" .. string.gsub(path, "::", "/")
    if type == "mod" then
        tgt = tgt .. "/"
    else
        tgt = string.gsub(tgt, "/([^/]*)$", "/" .. type .. ".%1.html")
    end
    return pandoc.Link(label, tgt)
end

function _load_schema(file)
    local path, base, ext, f, text
    path = pandoc.path.join({ quarto.project.directory, "..", file })
    base, ext = pandoc.path.split_extension(path)
    path = base .. ".json"
    quarto.log.debug("reading schema file", path)

    f = io.open(path)
    if f == nil then
        quarto.log.warning("could not open schema file", path)
        return nil
    end

    text = f:read("a")
    return quarto.json.decode(text)
end

function _schema_table(path, schema)
    local header = pandoc.Row({
        pandoc.Cell({ pandoc.Div("Field") }),
        pandoc.Cell({ pandoc.Div("Type") }),
    })
    local rows = pandoc.List()
    for _, field in ipairs(schema.fields) do
        rows[#rows + 1] = pandoc.Row({
            pandoc.Cell({ pandoc.Div(field.name) }),
            pandoc.Cell({ pandoc.Div(field.data_type) }),
        })
    end

    local caption = { "Schema for ", pandoc.Code(path), "." }
    return pandoc.Table(
        { long = { pandoc.Div({ caption }) }, short = caption },
        { { pandoc.AlignLeft, .6 }, { pandoc.AlignRight, .4 } },
        pandoc.TableHead({ header }),
        { { attr = pandoc.Attr(), body = rows, head = {}, row_head_columns = 0 } }, pandoc.TableFoot(),
        pandoc.Attr("", { "file-schema" })
    )
end

Div = function(el)
    local file = el.attributes['file']
    if file == nil then
        return el
    end
    local id = el.attr.identifier
    if id == "" then
        id = "file:" .. file
        el.attr.identifier = id
    end
    quarto.log.debug("found file", file, "in", quarto.doc.input_file, "with anchor", id)
    el.classes:extend({"file-block"})

    local header = pandoc.List({
        pandoc.Code(file)
    })
    local struct = el.attributes['struct']
    if struct ~= nil then
        header:extend({
            " (struct ",
            rust_link(struct, "struct"),
            ")"
        })
    end

    el.content:insert(1, pandoc.Div({header}, pandoc.Attr("", {"file-header"})))

    local meta = {}
    if el.classes:includes('parquet') then
        local schema = _load_schema(file)
        if schema == nil then
            meta = {
                pandoc.Div({
                    pandoc.Str("Could not find schema for "),
                    pandoc.Code(file),
                    pandoc.Str(".")
                }, pandoc.Attr("", { "callout-error" }, {}))
            }
        else
            meta = {
                pandoc.RawBlock("html", "<details class='file-details'><summary>File details</summary>"),
                _schema_table(file, schema),
                pandoc.RawBlock("html", "</details>"),
            }
        end
    end

    el.content:extend(meta)
    return el
end
